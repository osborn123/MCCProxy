package org.mccproxy.proxy;

import com.codahale.metrics.Counter;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.mccproxy.connector.db.PostgresConnectorTestIT;
import org.mccproxy.utils.TimeUtils;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MCCProxyEagerTestIT {

    MCCProxy mccProxy = null;

    @Test
    public void testConstructor() throws IOException, SQLException {
        RedisServer redisServer = new RedisServer(6379);
        redisServer.start();

        MCCProxy mccProxy = null;
        try (EmbeddedPostgres pg = EmbeddedPostgres.builder().setPort(63552)
                .setServerConfig("track_commit_timestamp", "on").start();
             Connection c = pg.getPostgresDatabase().getConnection()) {
            mccProxy = new MCCProxyEager(
                    MCCProxyServer.loadConfig("proxy-config.yaml"));
            mccProxy.start();

        } finally {
            mccProxy.stop();
            redisServer.stop();
        }

    }

    @Test
    public void testOrderedInvalidation() throws IOException, SQLException {
        RedisServer redisServer = new RedisServer(6379);
        redisServer.start();

        try (EmbeddedPostgres pg = EmbeddedPostgres.builder().setPort(63552)
                .setServerConfig("track_commit_timestamp", "on").start();
             Connection c = pg.getPostgresDatabase().getConnection()) {
            mccProxy = new MCCProxyEager(
                    MCCProxyServer.loadConfig("proxy-config.yaml"));
            mccProxy.start();

            PostgresConnectorTestIT.createTable(c);

            // insert DB data
            // now the data stored in DB is
            // key1: 1 (v1)
            // key2: 2 (v1)
            // key3: 3 (v1)
            // while the cache is empty
            var keys = List.of("key1", "key2", "key3");
            var values = List.of(1, 2, 3);
            var version1 = insertData(c, keys, values);
            mccProxy.processInvalidation(keys, version1);

            // Cache empty now, all misses
            var result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys, values,
                                                        List.of(version1,
                                                                version1,
                                                                version1));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(3, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // MCC Hit
            resetCounters();
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys, values,
                                                        List.of(version1,
                                                                version1,
                                                                version1));
            assertEquals(0, mccProxy.dbReadCounter.getCount());
            assertEquals(0, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(1, mccProxy.mccHitCounter.getCount());
            assertEquals(3, mccProxy.mccHitItemsCounter.getCount());

            // update DB data
            // now the data stored in DB is
            // key1: 1 (v1)
            // key2: 4 (v2)
            // key3: 9 (v2)
            // key4: 4 (v3)
            // while the data stored in cache is
            // key1: 1 (v1)
            // key2: 2 (v1 valid until v2)
            // key3: 3 (v1 valid until v2)
            var version2 =
                    updateData(c, List.of("key2", "key3"), List.of(4, 9));
            mccProxy.processInvalidation(List.of("key2", "key3"), version2);
            var version3 = insertData(c, List.of("key4"), List.of(4));
            mccProxy.processInvalidation(List.of("key4"), version3);

            // MCC hit
            resetCounters();
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys, values,
                                                        List.of(version1,
                                                                version1,
                                                                version1));
            assertEquals(0, mccProxy.dbReadCounter.getCount());
            assertEquals(0, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(1, mccProxy.mccHitCounter.getCount());
            assertEquals(3, mccProxy.mccHitItemsCounter.getCount());

            // miss
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 2 (v1 valid until v2)
            // key3: 9 (v2)
            // key4: 4 (v3)
            resetCounters();
            keys = List.of("key3", "key4");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(9, 4),
                                                        List.of(version2,
                                                                version3));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(2, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // miss
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 4 (v2)
            // key3: 9 (v2)
            // key4: 4 (v3)
            resetCounters();
            keys = List.of("key2", "key3");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(4, 9),
                                                        List.of(version2,
                                                                version2));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(1, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // update DB data
            // now the data stored in DB is
            // key1: 1 (v1)
            // key2: 16 (v5)
            // key3: 27 (v4)
            // key4: 4 (v3)
            // key5: 5 (v6)
            // while the data stored in cache is
            // key1: 1 (v1)
            // key2: 4 (v2 valid until v4)
            // key3: 9 (v2 valid until v4)
            // key4: 4 (v3)
            var version4 =
                    updateData(c, List.of("key2", "key3"), List.of(8, 27));
            mccProxy.processInvalidation(List.of("key2", "key3"), version4);
            var version5 = updateData(c, List.of("key2"), List.of(16));
            mccProxy.processInvalidation(List.of("key2"), version5);
            var version6 = insertData(c, List.of("key5"), List.of(5));
            mccProxy.processInvalidation(List.of("key5"), version6);

            // mcc hit
            resetCounters();
            keys = List.of("key1", "key2", "key3", "key4");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 4, 9, 4),
                                                        List.of(version1,
                                                                version2,
                                                                version2,
                                                                version3));
            assertEquals(0, mccProxy.dbReadCounter.getCount());
            assertEquals(0, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(1, mccProxy.mccHitCounter.getCount());
            assertEquals(4, mccProxy.mccHitItemsCounter.getCount());

            // miss
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 16 (v5)
            // key3: 9 (v2 valid until v4)
            // key4: 4 (v3)
            // key5: 5 (v6)
            resetCounters();
            keys = List.of("key1", "key2", "key5");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 16, 5),
                                                        List.of(version1,
                                                                version5,
                                                                version6));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(2, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());


            // miss
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 16 (v5)
            // key3: 9 (v4)
            // key4: 4 (v3)
            // key5: 5 (v6)
            resetCounters();
            keys = List.of("key1", "key2", "key3", "key4", "key5");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 16, 27, 4,
                                                                5),
                                                        List.of(version1,
                                                                version5,
                                                                version4,
                                                                version3,
                                                                version6));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(1, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

        } finally {
            mccProxy.stop();
            redisServer.stop();
        }

    }

    @Test
    public void testDisorderedInvalidation() throws IOException, SQLException {
        RedisServer redisServer = new RedisServer(6379);
        redisServer.start();

        try (EmbeddedPostgres pg = EmbeddedPostgres.builder().setPort(63552)
                .setServerConfig("track_commit_timestamp", "on").start();
             Connection c = pg.getPostgresDatabase().getConnection()) {
            mccProxy = new MCCProxyEager(
                    MCCProxyServer.loadConfig("proxy-config.yaml"));
            mccProxy.start();

            PostgresConnectorTestIT.createTable(c);

            // insert DB data
            // now the data stored in DB is
            // key1: 1 (v1)
            // key2: 2 (v1)
            // key3: 3 (v1)
            // while the cache is empty
            var keys = List.of("key1", "key2", "key3");
            var values = List.of(1, 2, 3);
            var version1 = insertData(c, keys, values);

            // Cache empty now, all misses
            resetCounters();
            var result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys, values,
                                                        List.of(version1,
                                                                version1,
                                                                version1));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(3, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // Invalidation outdated, Fetch all from DB
            // the data stored in cache is
            // key1: 1 (v1)
            // key2: 2 (v1)
            // key3: 3 (v1)
            resetCounters();
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys, values,
                                                        List.of(version1,
                                                                version1,
                                                                version1));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(3, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // update DB data
            // now the data stored in DB is
            // key1: 1 (v1)
            // key2: 4 (v2)
            // key3: 9 (v2)
            // key4: 4 (v3)
            // the data stored in cache is
            // key1: 1 (v1)
            // key2: 2 (v1)
            // key3: 3 (v1)
            var version2 =
                    updateData(c, List.of("key2", "key3"), List.of(4, 9));
            var version3 = insertData(c, List.of("key4"), List.of(4));

            // Invalidation outdated, Fetch all from DB
            // the data stored in cache is
            // key1: 1 (v1)
            // key2: 4 (v2)
            // key3: 9 (v2)
            resetCounters();
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 4, 9),
                                                        List.of(version1,
                                                                version2,
                                                                version2));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(3, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // miss
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 4 (v2)
            // key3: 9 (v2)
            // key4: 4 (v3)
            resetCounters();
            keys = List.of("key3", "key4");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(9, 4),
                                                        List.of(version2,
                                                                version3));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(2, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // miss
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 4 (v2)
            // key3: 9 (v2)
            // key4: 4 (v3)
            resetCounters();
            keys = List.of("key2", "key3");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(4, 9),
                                                        List.of(version2,
                                                                version2));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(2, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // update DB data
            // now the data stored in DB is
            // key1: 1 (v1)
            // key2: 16 (v5)
            // key3: 27 (v4)
            // key4: 4 (v3)
            // key5: 5 (v6)
            // while the data stored in cache is
            // key1: 1 (v1)
            // key2: 4 (v2)
            // key3: 9 (v2)
            // key4: 4 (v3)
            var version4 =
                    updateData(c, List.of("key2", "key3"), List.of(8, 27));
            var version5 = updateData(c, List.of("key2"), List.of(16));
            var version6 = insertData(c, List.of("key5"), List.of(5));

            // Invalidation outdated, Fetch all from DB
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 16 (v5)
            // key3: 27 (v4)
            // key4: 4 (v3)
            resetCounters();
            keys = List.of("key1", "key2", "key3", "key4");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 16, 27, 4),
                                                        List.of(version1,
                                                                version5,
                                                                version4,
                                                                version3));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(4, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // miss key5
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 16 (v5)
            // key3: 27 (v4)
            // key4: 4 (v3)
            // key5: 5 (v6)
            resetCounters();
            keys = List.of("key1", "key2", "key5");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 16, 5),
                                                        List.of(version1,
                                                                version5,
                                                                version6));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(3, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            // Invalidation outdated, Fetch all from DB
            // now the data stored in cache is
            // key1: 1 (v1)
            // key2: 16 (v5)
            // key3: 27 (v4)
            // key4: 4 (v3)
            // key5: 5 (v6)
            resetCounters();
            keys = List.of("key1", "key2", "key3", "key4", "key5");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 16, 27, 4,
                                                                5),
                                                        List.of(version1,
                                                                version5,
                                                                version4,
                                                                version3,
                                                                version6));
            assertEquals(1, mccProxy.dbReadCounter.getCount());
            assertEquals(5, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(0, mccProxy.mccHitCounter.getCount());
            assertEquals(0, mccProxy.mccHitItemsCounter.getCount());

            mccProxy.processInvalidation(List.of("key1", "key2", "key3"),
                                         version1);
            mccProxy.processInvalidation(List.of("key2", "key3"), version2);
            mccProxy.processInvalidation(List.of("key4"), version3);
            mccProxy.processInvalidation(List.of("key2", "key3"), version4);
            mccProxy.processInvalidation(List.of("key2"), version5);
            mccProxy.processInvalidation(List.of("key5"), version6);

            // mcc hit
            resetCounters();
            keys = List.of("key1", "key2", "key3", "key4", "key5");
            result = mccProxy.processRead(keys);
            PostgresConnectorTestIT.validateQueryResult(result, keys,
                                                        List.of(1, 16, 27, 4,
                                                                5),
                                                        List.of(version1,
                                                                version5,
                                                                version4,
                                                                version3,
                                                                version6));
            assertEquals(0, mccProxy.dbReadCounter.getCount());
            assertEquals(0, mccProxy.dbReadItemsCounter.getCount());
            assertEquals(1, mccProxy.mccHitCounter.getCount());
            assertEquals(5, mccProxy.mccHitItemsCounter.getCount());

        } finally {
            mccProxy.stop();
            redisServer.stop();
        }

    }

    private void resetCounters() {
        resetCounter(mccProxy.dbReadCounter);
        resetCounter(mccProxy.dbReadItemsCounter);
        resetCounter(mccProxy.mccHitCounter);
        resetCounter(mccProxy.mccHitItemsCounter);
    }

    private void resetCounter(Counter counter) {
        counter.dec(counter.getCount());
    }


    private long insertData(Connection c, List<String> keys,
                            List<Integer> values) throws SQLException {
        List<Long> versions = new ArrayList<>();
        try (Statement stmt = c.createStatement()) {
            // Prepare the SQL statement
            StringBuilder sql = new StringBuilder("INSERT INTO test VALUES ");
            for (int i = 0; i < keys.size(); i++) {
                sql.append(String.format("('%s', %d)", keys.get(i),
                                         values.get(i)));
                if (i < keys.size() - 1) {
                    sql.append(", ");
                }
            }

            // Execute the SQL statement
            stmt.execute(sql.toString());


            for (int i = 0; i < keys.size(); i++) {
                ResultSet rs = stmt.executeQuery(String.format(
                        "SELECT pg_xact_commit_timestamp(xmin) FROM test WHERE key = '%s'",
                        keys.get(i)));
                if (rs.next()) {
                    versions.add(TimeUtils.convertSqlTimestampToNanos(
                            rs.getTimestamp("pg_xact_commit_timestamp")));
                }
            }
        }
        return versions.getFirst();
    }

    private long updateData(Connection c, List<String> keys,
                            List<Integer> values) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            // Prepare the SQL statement
            StringBuilder sql =
                    new StringBuilder("UPDATE test SET value = CASE key ");
            for (int i = 0; i < keys.size(); i++) {
                sql.append(String.format("WHEN '%s' THEN %d ", keys.get(i),
                                         values.get(i)));
            }
            sql.append("END WHERE key IN (");
            for (int i = 0; i < keys.size(); i++) {
                sql.append(String.format("'%s'", keys.get(i)));
                if (i < keys.size() - 1) {
                    sql.append(", ");
                }
            }
            sql.append(")");

            // Execute the SQL statement
            stmt.execute(sql.toString());

            List<Long> versions = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                ResultSet rs = stmt.executeQuery(String.format(
                        "SELECT pg_xact_commit_timestamp(xmin) FROM test WHERE key = '%s'",
                        keys.get(i)));
                if (rs.next()) {
                    versions.add(TimeUtils.convertSqlTimestampToNanos(
                            rs.getTimestamp("pg_xact_commit_timestamp")));
                }
            }

            return versions.getFirst();
        }
    }
}
