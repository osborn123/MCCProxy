package org.mccproxy.connector.db;

import org.mccproxy.proxy.ItemRecord;

import java.util.List;

public interface DBConnector {
    void connect();

    void disconnect();

    List<ItemRecord> batchSelect(String table, List<String> keys);
}
