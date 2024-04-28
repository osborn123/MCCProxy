package org.mccproxy.connector.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

/**
 * A connector that connects to a Redis cache.
 */
public class RedisConnector implements CacheConnector {
    private String host;
    private int port;
    private Jedis jedis;

    public RedisConnector(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() {
        this.jedis = new Jedis(this.host, this.port);
        System.out.println("Connected to Redis cache at " + this.host + ":" + this.port);

    }

    public void disconnect() {
        if (this.jedis != null) {
            this.jedis.close();
            System.out.println("Disconnected from Redis cache at " + this.host + ":" + this.port);
        } else {
            System.out.println("No connection to Redis cache to disconnect from.");
        }
    }


    public void batchInsert(List<String> keys, List<String> values) {
        Pipeline pipeline = jedis.pipelined();
        for (int i = 0; i < keys.size(); i++) {
            pipeline.set(keys.get(i), values.get(i));
        }
        pipeline.sync();
        System.out.println("Inserted key-value pairs into Redis cache in batch");
    }

    public List<Response<String>> batchSelect(List<String> keys) {
        Pipeline pipeline = jedis.pipelined();
        List<Response<String>> responses = new ArrayList<>();
        for (String key : keys) {
            responses.add(pipeline.get(key));
        }
        pipeline.sync();
        System.out.println("Selected key-value pairs from Redis cache in batch");
        return responses;
    }

    public void batchDelete(List<String> keys) {
        Pipeline pipeline = jedis.pipelined();
        for (String key : keys) {
            pipeline.del(key);
        }
        pipeline.sync();
        System.out.println("Deleted key-value pairs from Redis cache in batch");
    }

}
