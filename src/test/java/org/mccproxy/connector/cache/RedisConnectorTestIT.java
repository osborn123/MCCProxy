package org.mccproxy.connector.cache;

import org.junit.jupiter.api.Test;
import org.mccproxy.proxy.ItemRecord;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RedisConnectorTestIT {

    @Test
    public void testBatchOperations() throws IOException {
        RedisServer redisServer = new RedisServer(6379);
        redisServer.start();
        RedisConnector redisConnector = new RedisConnector("localhost", 6379);
        redisConnector.connect();

        try {
            List<ItemRecord> putKeyValues =
                    List.of(new ItemRecord("key1", "value1"),
                            new ItemRecord("key2", "value2"),
                            new ItemRecord("key3", "value3"));

            var result1 = redisConnector.batchExecute(List.of(), List.of(),
                                                      putKeyValues);

            assertEquals(3, result1.size());
            assertEquals("OK", result1.get(0));
            assertEquals("OK", result1.get(1));
            assertEquals("OK", result1.get(2));

            var result2 =
                    redisConnector.batchExecute(List.of("key1", "key2", "key3"),
                                                List.of(), List.of());

            assertEquals(3, result2.size());
            assertEquals("value1", result2.get(0));
            assertEquals("value2", result2.get(1));
            assertEquals("value3", result2.get(2));


            var result3 = redisConnector.batchExecute(List.of(),
                                                      List.of("key1", "key2",
                                                              "key3"),
                                                      List.of());

            assertEquals(3, result3.size());
            assertEquals(1L, result3.get(0));
            assertEquals(1L, result3.get(1));
            assertEquals(1L, result3.get(2));

        } finally {
            redisConnector.disconnect();
            redisServer.stop();
        }
    }

    @Test
    void testBatchOperation2() throws IOException {
        RedisServer redisServer = new RedisServer(6379);
        redisServer.start();
        RedisConnector redisConnector = new RedisConnector("localhost", 6379);
        redisConnector.connect();

        try {
            List<ItemRecord> putKeyValues =
                    List.of(new ItemRecord("key1", "value1"),
                            new ItemRecord("key1", "value2"),
                            new ItemRecord("key3", "value3"));

            var result1 = redisConnector.batchExecute(List.of(), List.of(),
                                                      putKeyValues);

            assertEquals(3, result1.size());
            assertEquals("OK", result1.get(0));
            assertEquals("OK", result1.get(1));
            assertEquals("OK", result1.get(2));

            var result2 =
                    redisConnector.batchExecute(List.of("key1", "key2", "key3"),
                                                List.of("key1"), List.of());

            assertEquals(4, result2.size());
            assertEquals("value2", result2.get(0));
            assertNull(result2.get(1));
            assertEquals("value3", result2.get(2));
            assertEquals(1L, result2.get(3));

            var result3 = redisConnector.batchExecute(List.of("key1", "key3"),
                                                      List.of("key1"),
                                                      List.of());

            assertEquals(3, result3.size());
            assertNull(result3.get(0));
            assertEquals("value3", result3.get(1));
            assertEquals(0L, result3.get(2));

        } finally {
            redisConnector.disconnect();
            redisServer.stop();
        }
    }

    @Test
    void testBatchOperation3() throws IOException {
        RedisServer redisServer = new RedisServer(6379);
        redisServer.start();
        RedisConnector redisConnector = new RedisConnector("localhost", 6379);
        redisConnector.connect();

        try {
            List<ItemRecord> putKeyValues =
                    List.of(new ItemRecord("key1", "value1"),
                            new ItemRecord("key1", "value2"),
                            new ItemRecord("key3", "value3"));

            var result1 = redisConnector.batchExecute(List.of(), List.of(),
                                                      putKeyValues);

            assertEquals(3, result1.size());
            assertEquals("OK", result1.get(0));
            assertEquals("OK", result1.get(1));
            assertEquals("OK", result1.get(2));

            redisServer.stop();

            var result2 =
                    redisConnector.batchExecute(List.of("key1", "key2", "key3"),
                                                List.of("key1"), List.of());

            assertNull(result2);
        } finally {
            redisConnector.disconnect();
            redisServer.stop();
        }
    }
}