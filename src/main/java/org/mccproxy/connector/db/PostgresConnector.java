package org.mccproxy.connector.db;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresConnector implements DBConnector {
    static private Logger logger = LoggerFactory.getLogger(PostgresConnector.class.getName());
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

    public void connect() {
        try {
            this.connection = DriverManager.getConnection(this.url, this.user, this.password);
            logger.info("Connected to PostgreSQL database at " + this.url);
        } catch (SQLException e) {
            logger.info("Failed to connect to PostgreSQL database at " + this.url, e.getCause());
        }
    }

    public void disconnect() {
        if (this.connection != null) {
            try {
                this.connection.close();
                logger.info("Disconnected from PostgreSQL database at " + this.url);
            } catch (SQLException e) {
                logger.info("Failed to disconnect from PostgreSQL database at " + this.url, e.getCause());
            }
        }
    }

    public List<String> batchSelect(String table, List<String> keys) {
        List<String> results = new ArrayList<>();
        try (Statement stmt = this.connection.createStatement()) {
            for (String key : keys) {
                String sql = String.format("SELECT value FROM %s WHERE %s = '%s'", table, "key", key);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next()) {
                        results.add(rs.getString("value"));
                    }
                }
            }
            logger.info("Selected key-value pairs from PostgreSQL database in batch");
        } catch (SQLException e) {
            logger.info("Failed to select key-value pairs from PostgreSQL database in batch", e.getCause());
            e.printStackTrace();
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