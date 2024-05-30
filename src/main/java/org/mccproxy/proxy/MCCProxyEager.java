package org.mccproxy.proxy;

import org.mccproxy.cache.ConsistentCache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MCCProxyEager extends MCCProxy {
    private final ConsistentCache cache;

    public MCCProxyEager(MCCProxyConfig configFilePath) {
        super(configFilePath);
        this.cache = new ConsistentCache(1000);
    }

    @Override
    public List<ItemRecord> processRead(List<String> keys) {
        timeStep++;

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
                        cache.getObsoleteItems(decidedItems, this.timeStep);
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

        this.cache.postCacheUpdate(hitItems, itemsToEvict, itemsFromDb,
                                   this.timeStep);

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

    @Override
    public boolean processInvalidation(List<String> keys, long newVersion) {
        timeStep++;
        for (String key : keys) {
            cache.invalidate(key, newVersion, timeStep);
        }
        return true;
    }
}
