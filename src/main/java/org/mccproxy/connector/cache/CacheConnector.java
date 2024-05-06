package org.mccproxy.connector.cache;

import org.mccproxy.proxy.ItemRecord;

import java.util.List;

public interface CacheConnector {
    void connect();

    void disconnect();

    List<Object> batchExecute(List<String> selectKeys, List<String> deleteKeys,
                              List<ItemRecord> putKeyValues);

    void batchInsert(List<String> keys, List<String> values);

    List<String> batchSelect(List<String> keys);
}
