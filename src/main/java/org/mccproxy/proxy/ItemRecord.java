package org.mccproxy.proxy;

public class ItemRecord {
    private final String key;
    private final String value;
    private final long version;

    public ItemRecord(String key, String value) {
        this.key = key;
        this.value = value;
        this.version = 0;
    }

    public ItemRecord(String key, String value, long version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    public int getSize() {
        return key.length() + value.length();
    }

    public String toString() {
        return "ItemRecord{" + "key='" + key + '\'' + ", value='" + value +
                '\'' + ", version=" + version + '}';
    }
}
