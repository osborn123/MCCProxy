package org.mccproxy.proxy;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import org.mccproxy.cache.ConsistentCache;
import org.mccproxy.connector.cache.CacheConnector;
import org.mccproxy.connector.cache.RedisConnector;
import org.mccproxy.connector.db.DBConnector;
import org.mccproxy.connector.db.PostgresConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.*;

public class MCCProxy {
    private static final Logger logger =
            LoggerFactory.getLogger(MCCProxy.class.getName());
    private static final MetricRegistry metrics = new MetricRegistry();
    @VisibleForTesting
    final Counter mccHitCounter;
    @VisibleForTesting
    final Counter dbReadCounter;
    @VisibleForTesting
    final Counter mccHitItemsCounter;
    @VisibleForTesting
    final Counter dbReadItemsCounter;

    private final ConsistentCache cache;
    private final CacheConnector cacheConnector;
    private final DBConnector dbConnector;
    private final MCCProxyConfig config;
    private final HashMap<String, Long> itemsLastReadVersion;
    private final boolean testMode = true;

    public MCCProxy(String configFilePath) {
        logger.info("MCCProxy::MCCProxy - Loading configuration from: {}",
                    configFilePath);

        Yaml yaml = new Yaml(
                new Constructor(MCCProxyConfig.class, new LoaderOptions()));
        InputStream inputStream =
                getClass().getClassLoader().getResourceAsStream(configFilePath);
        config = yaml.load(inputStream);

        logger.info("MCCProxy::MCCProxy - Configuration loaded: {}", config);

        this.cache = new ConsistentCache(1000);
        this.cacheConnector = new RedisConnector(config.getRedis().getHost(),
                                                 config.getRedis().getPort());
        this.dbConnector = new PostgresConnector(config.getPostgres().getUrl(),
                                                 config.getPostgres().getUser(),
                                                 config.getPostgres()
                                                         .getPassword());

        mccHitCounter =
                metrics.counter(MetricRegistry.name(MCCProxy.class, "mccHit"));
        dbReadCounter =
                metrics.counter(MetricRegistry.name(MCCProxy.class, "dbRead"));
        mccHitItemsCounter = metrics.counter(
                MetricRegistry.name(MCCProxy.class, "mccHitItems"));
        dbReadItemsCounter = metrics.counter(
                MetricRegistry.name(MCCProxy.class, "dbReadItems"));

        if (testMode) {
            itemsLastReadVersion = new HashMap<>();
        } else {
            itemsLastReadVersion = null;
        }
    }

    public void start() {
        logger.info("MCCProxy::start - Starting MCCProxy");
        this.cacheConnector.connect();
        this.dbConnector.connect();
    }

    public void stop() {
        logger.info("MCCProxy::stop - Stopping MCCProxy");
        this.cacheConnector.disconnect();
        this.dbConnector.disconnect();
    }

    public List<ItemRecord> processRead(List<String> keys) {
        logger.info("MCCProxy::processRead - Processing read for keys: {}",
                    keys);
        if (keys.isEmpty()) {
            return new ArrayList<>();
        }

        boolean isMCCHit = this.cache.isMCCHit(keys);
        List<String> missingItems;
        List<String> outdatedItems;
        List<String> hitItems;
        if (isMCCHit) {
            missingItems = new ArrayList<>();
            outdatedItems = new ArrayList<>();
            hitItems = new ArrayList<>(keys);

            cache.markItems(keys);

            logger.info("MCCProxy::processRead - MCC hit");
        } else {
            ConsistentCache.MCCHitResult mccHitResult =
                    this.cache.makeMCCHit(keys);
            missingItems = mccHitResult.getMissingItems();
            outdatedItems = mccHitResult.getOutdatedItems();
            hitItems = mccHitResult.getHitItems();

            logger.info(
                    "MCCProxy::processRead - missing items: {} outdated items: {} hit items: {}",
                    missingItems, outdatedItems, hitItems);
        }

        long maxInvalidationTimestamp =
                this.cache.getMaxInvalidationTimestamp();

        // if items are invalidated here
        // 2 - 5, 3 - inf, 4 - 5
        // 2 - 5, 3 - 7, 4 - 5
        // 7 - inf, 3 - 7, 7 - inf
        List<String> itemsToReadFromDB = new ArrayList<>();
        itemsToReadFromDB.addAll(missingItems);
        itemsToReadFromDB.addAll(outdatedItems);
        List<ItemRecord> itemsFromDb;
        if (!itemsToReadFromDB.isEmpty()) {
            itemsFromDb =
                    this.dbConnector.batchSelect("test", itemsToReadFromDB);

            logger.info("MCCProxy::processRead - Read items from DB: {}",
                        itemsFromDb);
            dbReadCounter.inc();
            dbReadItemsCounter.inc(itemsToReadFromDB.size());
        } else {
            itemsFromDb = new ArrayList<>();
        }

        if (!hitItems.isEmpty()) {
            // Determine the maximum version among the items fetched from the database
            long maxItemsVersion = 0;
            for (ItemRecord item : itemsFromDb) {
                maxItemsVersion = Math.max(maxItemsVersion, item.getVersion());
            }
            for (String key : hitItems) {
                maxItemsVersion = Math.max(maxItemsVersion,
                                           this.cache.getItemVersion(key));
            }

            logger.info(
                    "MCCProxy::processRead - maxInvalidationTimestamp: {} maxItemsVersion: {}",
                    maxInvalidationTimestamp, maxItemsVersion);

            //            // Check if the invalidation is up-to-date
            //            boolean invalidationUpToDate =
            //                    maxInvalidationTimestampBefore >= maxFetchedItemsVersion;

            //            // We need to check the validity of each item in the hit items
            //            if (!needToFetchAll && !invalidationUpToDate) {
            //                for (String key : hitItems) {
            //                    if (this.cache.getItemValidUntil(key) <=
            //                            maxFetchedItemsVersion) {
            //                        needToFetchAll = true;
            //                        break;
            //                    }
            //                }
            //            }

            // if invalidation is not up-to-date, there can be items not invalidated
            // read all items from DB
            if (maxItemsVersion > maxInvalidationTimestamp) {
                itemsFromDb = this.dbConnector.batchSelect("test", keys);
                dbReadCounter.inc();
                dbReadItemsCounter.inc(keys.size());
                outdatedItems.addAll(hitItems);
                hitItems.clear();
                logger.info(
                        "MCCProxy::processRead - Read all items from DB: {}",
                        itemsFromDb);
            }
        }

        if (hitItems.size() == keys.size()) {
            this.mccHitCounter.inc();
            this.mccHitItemsCounter.inc(keys.size());
        }

        int dataSizeToPut = 0;
        for (ItemRecord item : itemsFromDb) {
            dataSizeToPut += item.getSize();
        }
        logger.info("MCCProxy::processRead - Data size to put: {}",
                    dataSizeToPut);

        // evict outdated items
        int outdatedItemsSize = cache.getDataSize(outdatedItems);
        logger.info("MCCProxy::processRead - Outdated items: {} size: {}",
                    outdatedItems, outdatedItemsSize);

        List<String> itemsToEvict = new ArrayList<>();
        // evict obsolete items
        if (cache.getCacheSize() + dataSizeToPut - outdatedItemsSize >
                cache.getCacheSizeLimit()) {
            int neededSize =
                    cache.getCacheSize() + dataSizeToPut - outdatedItemsSize -
                            cache.getCacheSizeLimit();

            Set<String> decidedItems = new HashSet<>();
            decidedItems.addAll(hitItems);
            decidedItems.addAll(outdatedItems);

            if (cache.isAllMarked()) {
                cache.startNewPhase();

                List<String> obsoleteItems =
                        cache.getObsoleteItems(decidedItems);
                int obsoleteItemsSize = cache.getDataSize(obsoleteItems);
                itemsToEvict.addAll(obsoleteItems);
                neededSize -= obsoleteItemsSize;

                logger.info(
                        "MCCProxy::processRead - Obsolete items: {} size: {}",
                        obsoleteItems, obsoleteItemsSize);
            }

            logger.info("MCCProxy::processRead - Needed size: {}", neededSize);

            if (neededSize > 0) {
                decidedItems.addAll(itemsToEvict);
                List<String> lruItems =
                        cache.getLruItems(decidedItems, neededSize);

                logger.info("MCCProxy::processRead - LRU items: {}", lruItems);

                itemsToEvict.addAll(lruItems);
            }
        }

        List<Object> cacheResults =
                this.cacheConnector.batchExecute(hitItems, itemsToEvict,
                                                 itemsFromDb);

        logger.info("MCCProxy::processRead - Cache execution results: {}",
                    cacheResults);

        this.cache.postCacheUpdate(hitItems, itemsToEvict, itemsFromDb);

        List<ItemRecord> results = new ArrayList<>();
        for (int i = 0; i < hitItems.size(); i++) {
            String key = hitItems.get(i);
            results.add(new ItemRecord(key, cacheResults.get(i).toString(),
                                       cache.getItemVersion(key)));
        }
        results.addAll(itemsFromDb);

        logger.info("MCCProxy::processRead - Returning results: {}", results);

        if (testMode) {
            for (ItemRecord item : results) {
                assert !itemsLastReadVersion.containsKey(item.getKey()) ||
                        itemsLastReadVersion.get(item.getKey()) <=
                                item.getVersion();
                itemsLastReadVersion.put(item.getKey(), item.getVersion());
            }
        }
        return results;
    }

    public boolean processInvalidation(List<String> keys, long newVersion) {
        for (String key : keys) {
            cache.invalidate(key, newVersion);
        }
        return true;
    }


}
