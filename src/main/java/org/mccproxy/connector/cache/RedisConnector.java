package org.mccproxy.connector.cache;

import org.mccproxy.proxy.ItemRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * A connector that connects to a Redis cache.
 */
public class RedisConnector implements CacheConnector {
    private static Logger logger =
            LoggerFactory.getLogger(RedisConnector.class.getName());

    private String host;
    private int port;
    private Jedis jedis;

    public RedisConnector(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void connect() {
        this.jedis = new Jedis(this.host, this.port);
        logger.info("Connected to Redis cache at {}:{}", this.host, this.port);
    }

    @Override
    public void disconnect() {
        if (this.jedis != null) {
            this.jedis.close();
            logger.info("Disconnected from Redis cache at {}:{}", this.host,
                        this.port);
        } else {
            logger.info("No connection to Redis cache to disconnect from.");
        }
    }

    @Override
    public List<Object> batchExecute(List<String> selectKeys,
                                     List<String> deleteKeys,
                                     List<ItemRecord> putKeyValues) {
        List<Object> responses = null;
        try {
            Transaction transaction = jedis.multi();
            for (String key : selectKeys) {
                transaction.get(key);
            }
            for (String key : deleteKeys) {
                transaction.del(key);
            }
            for (ItemRecord putKeyValue : putKeyValues) {
                transaction.set(putKeyValue.getKey(), putKeyValue.getValue());
            }
            responses = transaction.exec();
            logger.info("Executed batch operation on Redis cache {}",
                        responses);
        } catch (Exception e) {
            logger.error("Failed to execute batch operation on Redis cache", e);
        }
        return responses;
    }

    @Override
    public void batchInsert(List<String> keys, List<String> values) {
        Pipeline pipeline = jedis.pipelined();
        for (int i = 0; i < keys.size(); i++) {
            pipeline.set(keys.get(i), values.get(i));
        }
        pipeline.sync();
        logger.info("Inserted key-value pairs into Redis cache in batch");
    }

    @Override
    public List<String> batchSelect(List<String> keys) {
        Pipeline pipeline = jedis.pipelined();
        List<Response<String>> responses = new ArrayList<>();
        for (String key : keys) {
            responses.add(pipeline.get(key));
        }
        pipeline.sync();
        logger.info("Selected key-value pairs from Redis cache in batch");
        List<String> results = new ArrayList<>();
        for (Response<String> response : responses) {
            results.add(response.get());
        }
        return results;
    }

    public void batchDelete(List<String> keys) {
        Pipeline pipeline = jedis.pipelined();
        for (String key : keys) {
            pipeline.del(key);
        }
        pipeline.sync();
        logger.info("Deleted key-value pairs from Redis cache in batch");
    }

}
