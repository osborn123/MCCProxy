package org.mccproxy.proxy;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import org.mccproxy.connector.cache.CacheConnector;
import org.mccproxy.connector.cache.RedisConnector;
import org.mccproxy.connector.db.DBConnector;
import org.mccproxy.connector.db.PostgresConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public abstract class MCCProxy {
    protected static final Logger logger =
            LoggerFactory.getLogger(MCCProxy.class.getName());
    protected static final MetricRegistry metrics = new MetricRegistry();
    protected final CacheConnector cacheConnector;
    protected final DBConnector dbConnector;

    protected final HashMap<String, Long> itemsLastReadVersion;
    protected final boolean testMode = true;
    @VisibleForTesting
    final Counter mccHitCounter;
    @VisibleForTesting
    final Counter dbReadCounter;
    @VisibleForTesting
    final Counter mccHitItemsCounter;
    @VisibleForTesting
    final Counter dbReadItemsCounter;

    protected long timeStep = 0;


    public MCCProxy(MCCProxyConfig config) {
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

    abstract public List<ItemRecord> processRead(List<String> keys);

    abstract public boolean processInvalidation(List<String> keys,
                                                long newVersion);
}
