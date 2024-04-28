package org.mccproxy.connector.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresConnectorTestIT {

    private void prepareTable(Connection c) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            stmt.execute("CREATE TABLE test (key TEXT PRIMARY KEY, value TEXT)");
            insertData(c, List.of("key1", "key2", "key3"), List.of("value1", "value2", "value3"));
        }
    }

    private List<Timestamp> insertData(Connection c, List<String> keys, List<String> values) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            for (int i = 0; i < keys.size(); i++) {
                stmt.execute(String.format("INSERT INTO test VALUES ('%s', '%s')", keys.get(i), values.get(i)));
            }

            List<Timestamp> versions = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                ResultSet rs = stmt.executeQuery(String.format("SELECT pg_xact_commit_timestamp(xmin) FROM test WHERE key = '%s'", keys.get(i)));
                if (rs.next()) {
                    versions.add(rs.getTimestamp("pg_xact_commit_timestamp"));
                }
            }
            return versions;
        }
    }


    private List<Timestamp> updateData(Connection c, List<String> keys, List<String> values) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            for (int i = 0; i < keys.size(); i++) {
                // use String.format
                stmt.execute(String.format("UPDATE test SET value = '%s' WHERE key = '%s'", values.get(i), keys.get(i)));
            }

            List<Timestamp> versions = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                ResultSet rs = stmt.executeQuery(String.format("SELECT pg_xact_commit_timestamp(xmin) FROM test WHERE key = '%s'", keys.get(i)));
                if (rs.next()) {
                    versions.add(rs.getTimestamp("pg_xact_commit_timestamp"));
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
            prepareTable(c);
            var connector = new PostgresConnector(c);
            var results = connector.batchSelect("test", List.of("key1", "key2", "key3"));
            assert results.size() == 3;
            assert results.get(0).equals("value1");
            assert results.get(1).equals("value2");
            assert results.get(2).equals("value3");
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
