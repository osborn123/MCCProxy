package org.mccproxy.cache;

import java.util.HashMap;

public class ConsistentCache {

    private class ItemNode {
        private String key;
        private int version;
        private ItemNode next;
        private ItemNode prev;
    }

    HashMap<String, ItemNode> cahcedItems;

    ItemNode dummyHead, dummyTail;

    public ConsistentCache() {
        cahcedItems = new HashMap<>();
        dummyHead = new ItemNode();
        dummyTail = new ItemNode();
        dummyHead.next = dummyTail;
        dummyTail.prev = dummyHead;
    }

    /**
     * Put a key into the cache with a version. If the key already exists, update the version. Otherwise, add the key to the cache.
     * Make the accessed node the head of the list.
     * @param key
     * @param version
     */
    public void put(String key, int version) {
        if (cahcedItems.containsKey(key)) {
            ItemNode node = cahcedItems.get(key);
            node.version = version;
            removeNode(node);
            addNode(node);
        } else {
            ItemNode node = new ItemNode();
            node.key = key;
            node.version = version;
            addNode(node);
            cahcedItems.put(key, node);
        }
    }


    private void removeNode(ItemNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void addNode(ItemNode node) {
        node.prev = dummyHead;
        node.next = dummyHead.next;
        dummyHead.next.prev = node;
        dummyHead.next = node;
    }

    /**
     * Get the version of the key in the cache. If the key does not exist in the cache, return -1.
     * Make the accessed node the head of the list.
     * @param key
     * @return
     */
    public int get(String key) {
        if (cahcedItems.containsKey(key)) {
            ItemNode node = cahcedItems.get(key);
            removeNode(node);
            addNode(node);
            return node.version;
        }
        return -1;
    }
}
