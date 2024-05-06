package org.mccproxy.connector.db;

import com.google.common.annotations.VisibleForTesting;
import org.mccproxy.proxy.ItemRecord;
import org.mccproxy.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresConnector implements DBConnector {
    static private Logger logger =
            LoggerFactory.getLogger(PostgresConnector.class.getName());
    private String url;
    private String user;
    private String password;
    private Connection connection;

    public PostgresConnector(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @VisibleForTesting
    PostgresConnector(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void connect() {
        try {
            this.connection = DriverManager.getConnection(this.url, this.user,
                                                          this.password);
            logger.info("Connected to PostgreSQL database at {}", this.url);
        } catch (SQLException e) {
            logger.info("Failed to connect to PostgreSQL database at {}",
                        this.url, e.getCause());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void disconnect() {
        if (this.connection != null) {
            try {
                this.connection.close();
                logger.info("Disconnected from PostgreSQL database at {}",
                            this.url);
            } catch (SQLException e) {
                logger.info(
                        "Failed to disconnect from PostgreSQL database at {}",
                        this.url, e.getCause());
            }
        }
    }
    //
    //    @Override
    //    public List<ItemRecord> batchSelect(String table, List<String> keys) {
    //        List<ItemRecord> results = new ArrayList<>();
    //        try (Statement stmt = this.connection.createStatement()) {
    //
    //            for (String key : keys) {
    //                String sql = String.format(
    //                        "SELECT key, value, pg_xact_commit_timestamp(xmin) as version FROM %s WHERE %s = '%s'",
    //                        table, "key", key);
    //                try (ResultSet rs = stmt.executeQuery(sql)) {
    //                    if (rs.next()) {
    //                        ItemRecord record = new ItemRecord(rs.getString("key"),
    //                                                           rs.getString(
    //                                                                   "value"),
    //                                                           Utils.convertToProtoTimestamp(
    //                                                                   rs.getTimestamp(
    //                                                                           "version")));
    //                        results.add(record);
    //
    //                    }
    //                }
    //            }
    //            logger.info(
    //                    "Selected key-value pairs from PostgreSQL database in batch");
    //        } catch (SQLException e) {
    //            logger.info(
    //                    "Failed to select key-value pairs from PostgreSQL database in batch",
    //                    e.getCause());
    //        }
    //        return results;
    //    }


    @Override
    public List<ItemRecord> batchSelect(String table, List<String> keys) {
        List<ItemRecord> results = new ArrayList<>();
        try (Statement stmt = this.connection.createStatement()) {
            String joinedKeys = String.join("', '",
                                            keys); // Join the keys with ', ' and wrap each key in single quotes
            String sql = String.format(
                    "SELECT key, value, pg_xact_commit_timestamp(xmin) as version FROM %s WHERE key IN ('%s')",
                    table, joinedKeys);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    ItemRecord record = new ItemRecord(rs.getString("key"),
                                                       rs.getString("value"),
                                                       TimeUtils.convertSqlTimestampToNanos(
                                                               rs.getTimestamp(
                                                                       "version")));
                    results.add(record);
                }
            }
            logger.info(
                    "Selected key-value pairs from PostgreSQL database in batch");
        } catch (SQLException e) {
            logger.info(
                    "Failed to select key-value pairs from PostgreSQL database in batch",
                    e.getCause());
        }
        return results;
    }

    //    public List<String> batchSelect(String table, List<String> keys, List<Timestamp> versions) {
    //        List<String> results = new ArrayList<>();
    //        try (Statement stmt = this.connection.createStatement()) {
    //            for (int i = 0; i < keys.size(); i++) {
    //                String sql = String.format("SELECT value FROM %s WHERE %s = '%s' and pg_xact_commit_timestamp(xmin) = '%s'",
    //                        table, "key", keys.get(i), versions.get(i));
    //                try (ResultSet rs = stmt.executeQuery(sql)) {
    //                    if (rs.next()) {
    //                        results.add(rs.getString("value"));
    //                    }
    //                }
    //            }
    //            System.out.println("Selected key-value pairs from PostgreSQL database in batch");
    //        } catch (SQLException e) {
    //            System.out.println("Failed to select key-value pairs from PostgreSQL database in batch");
    //            e.printStackTrace();
    //        }
    //        return results;
    //    }

}