package org.mccproxy.connector.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.mccproxy.proxy.ItemRecord;
import org.mccproxy.utils.TimeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgresConnectorTestIT {

    static public void createTable(Connection c) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            stmt.execute("CREATE TABLE test (key TEXT PRIMARY KEY, value INT)");
        }
    }

    static public void validateQueryResult(List<ItemRecord> queryResult,
                                           List<String> keys,
                                           List<Integer> values,
                                           List<Long> versions) {
        var resultMap = new HashMap<String, ItemRecord>();
        for (var item : queryResult) {
            resultMap.put(item.getKey(), item);
        }

        for (int i = 0; i < keys.size(); i++) {
            assertTrue(resultMap.containsKey(keys.get(i)));
            assertEquals(String.valueOf(values.get(i)),
                         resultMap.get(keys.get(i)).getValue());
            assertEquals(versions.get(i),
                         resultMap.get(keys.get(i)).getVersion());

        }
    }

    private List<Long> insertData(Connection c, List<String> keys,
                                  List<Integer> values) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            for (int i = 0; i < keys.size(); i++) {
                stmt.execute(String.format("INSERT INTO test VALUES ('%s', %d)",
                                           keys.get(i), values.get(i)));
            }

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
            return versions;
        }
    }

    private List<Long> updateData(Connection c, List<String> keys,
                                  List<Integer> values) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            for (int i = 0; i < keys.size(); i++) {
                stmt.execute(String.format(
                        "UPDATE test SET value = %d WHERE key = '%s'",
                        values.get(i), keys.get(i)));
            }

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
            return versions;
        }
    }

    @Test
    public void testBatchSelect() throws IOException, SQLException {
        try (EmbeddedPostgres pg = EmbeddedPostgres.builder()
                .setServerConfig("track_commit_timestamp", "on").start();
             Connection c = pg.getPostgresDatabase().getConnection()) {
            System.out.println(pg.getJdbcUrl("postgres", "postgres"));
            createTable(c);
            var keys = List.of("key1", "key2", "key3");
            var values = List.of(1, 2, 3);
            var versions = insertData(c, keys, values);

            var connector = new PostgresConnector(c);
            var results = connector.batchSelect("test", keys);

            validateQueryResult(results, keys, values, versions);

            var newValues = List.of(11, 22, 33);
            var newVersions = updateData(c, keys, newValues);
            var results2 = connector.batchSelect("test", keys);
            validateQueryResult(results2, keys, newValues, newVersions);
        }
    }


    //    @Test
    //    public void testBatchSelectVersion() throws IOException, SQLException {
    //        try (EmbeddedPostgres pg = EmbeddedPostgres.builder()
    //                .setServerConfig("track_commit_timestamp", "on").start();
    //             Connection c = pg.getPostgresDatabase().getConnection()) {
    //            prepareTable(c);
    //            var connector = new PostgresConnector(c);
    //            var versions = updateData(c, List.of("key1", "key2", "key3"), List.of("value11", "value22", "value33"));
    //            var results = connector.batchSelect("test", List.of("key1", "key2", "key3"));
    //            assert results.size() == 3;
    //            assert results.get(0).equals("value11");
    //            assert results.get(1).equals("value22");
    //            assert results.get(2).equals("value33");
    //
    //            updateData(c, List.of("key1", "key2", "key3"), List.of("value111", "value222", "value333"));
    //            var results2 = connector.batchSelect("test", List.of("key1", "key2", "key3"), versions);
    //            assert results2.size() == 3;
    //            assert results2.get(0).equals("value11");
    //            assert results2.get(1).equals("value22");
    //            assert results2.get(2).equals("value33");
    //        }}
    //    }
}
