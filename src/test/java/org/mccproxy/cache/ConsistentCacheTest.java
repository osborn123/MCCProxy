package org.mccproxy.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ConsistentCacheTest {
    private ConsistentCache cache;

    @BeforeEach
    public void setup() {
        cache = new ConsistentCache(100);
    }

    @Test
    public void testPut() throws JsonProcessingException {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey1"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 20};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        assertEquals(40, cache.getCacheSize());
        String jsonStr =
                "[{\"key\":\"testKey1\",\"version\":4,\"validUntil\":9223372036854775807,\"dataSize\":20}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":9223372036854775807,\"dataSize\":10}]";

        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);
    }

    @Test
    public void testEvict() throws JsonProcessingException {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey1"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 20};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        cache.evict("testKey2");
        assertEquals(30, cache.getCacheSize());
        String jsonStr =
                "[{\"key\":\"testKey1\",\"version\":4,\"validUntil\":9223372036854775807,\"dataSize\":20}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        cache.evict("testKey1");
        assertEquals(10, cache.getCacheSize());
        jsonStr =
                "[{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        cache.evict("testKey3");
        assertEquals(0, cache.getCacheSize());
        jsonStr = "[]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);
    }

    @Test
    public void testAccess() throws JsonProcessingException {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey1"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 20};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        cache.access("testKey2");
        String jsonStr =
                "[{\"key\":\"testKey2\",\"version\":2,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey1\",\"version\":4,\"validUntil\":9223372036854775807,\"dataSize\":20}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        cache.access("testKey1");
        jsonStr =
                "[{\"key\":\"testKey1\",\"version\":4,\"validUntil\":9223372036854775807,\"dataSize\":20}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);
    }

    @Test
    public void testGetDataSize() {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey1"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 20};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        assertEquals(0, cache.getDataSize(List.of()));
        assertEquals(20, cache.getDataSize(List.of("testKey1")));
        assertEquals(30, cache.getDataSize(List.of("testKey1", "testKey2")));
        assertEquals(20, cache.getDataSize(List.of("testKey2", "testKey3")));
        assertEquals(30, cache.getDataSize(List.of("testKey1", "testKey3")));
        assertEquals(40, cache.getDataSize(
                List.of("testKey1", "testKey2", "testKey3")));
    }

    @Test
    public void testInvalidate() throws JsonProcessingException {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey1"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 20};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        cache.invalidate("testKey2", 2L);
        String jsonStr =
                "[{\"key\":\"testKey1\",\"version\":4,\"validUntil\":9223372036854775807,\"dataSize\":20}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":9223372036854775807,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);
        assertEquals(2, cache.getMaxInvalidationTimestamp());

        cache.invalidate("testKey2", 5L);
        jsonStr =
                "[{\"key\":\"testKey1\",\"version\":4,\"validUntil\":9223372036854775807,\"dataSize\":20}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":5,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);
        assertEquals(5, cache.getMaxInvalidationTimestamp());

        cache.invalidate("testKey2", 7L);
        jsonStr =
                "[{\"key\":\"testKey1\",\"version\":4,\"validUntil\":9223372036854775807,\"dataSize\":20}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":5,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);
        assertEquals(7, cache.getMaxInvalidationTimestamp());
    }

    @Test
    public void testGetLruItems() {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey4"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 10};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        cache.access("testKey4");
        cache.access("testKey3");
        cache.access("testKey2");
        cache.access("testKey1");

        List<String> lruItems = cache.getLruItems(
                Set.of("testKey1", "testKey2", "testKey3", "testKey4"), 10);
        assertEquals(0, lruItems.size());

        lruItems = cache.getLruItems(Set.of("testKey1", "testKey4"), 10);
        assertEquals(1, lruItems.size());
        assertEquals("testKey3", lruItems.getFirst());

        lruItems = cache.getLruItems(Set.of("testKey1", "testKey4"), 15);
        assertEquals(2, lruItems.size());
        assertEquals("testKey3", lruItems.getFirst());
        assertEquals("testKey2", lruItems.get(1));
    }

    @Test
    public void testIsMCCHit() throws JsonProcessingException {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey4"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 10};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        cache.invalidate("testKey1", 5L);
        cache.invalidate("testKey2", 5L);
        cache.invalidate("testKey3", 5L);
        cache.invalidate("testKey4", 5L);
        String jsonStr =
                "[{\"key\":\"testKey4\",\"version\":4,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey1\",\"version\":1,\"validUntil\":5,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        assertTrue(cache.isMCCHit(List.of("testKey1")));
        assertFalse(cache.isMCCHit(List.of("testKey1", "testKey5")));
        assertTrue(cache.isMCCHit(
                List.of("testKey1", "testKey2", "testKey3", "testKey4")));

        cache.put("testKey1", 5L, 10);
        jsonStr =
                "[{\"key\":\"testKey1\",\"version\":5,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey4\",\"version\":4,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":5,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        assertTrue(cache.isMCCHit(List.of("testKey1")));
        assertFalse(cache.isMCCHit(List.of("testKey1", "testKey5")));
        assertFalse(cache.isMCCHit(List.of("testKey1", "testKey2")));
        assertFalse(cache.isMCCHit(
                List.of("testKey1", "testKey2", "testKey3", "testKey4")));
    }

    @Test
    public void testMakeMCCHit() throws JsonProcessingException {
        String[] keys = {"testKey1", "testKey2", "testKey3", "testKey4"};
        long[] versions = {1L, 2L, 3L, 4L};
        int[] dataSizes = {10, 10, 10, 10};

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], versions[i], dataSizes[i]);
        }

        cache.invalidate("testKey1", 5L);
        cache.invalidate("testKey2", 5L);
        cache.invalidate("testKey3", 5L);
        cache.invalidate("testKey4", 5L);
        String jsonStr =
                "[{\"key\":\"testKey4\",\"version\":4,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey1\",\"version\":1,\"validUntil\":5,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        var result = cache.makeMCCHit(
                List.of("testKey1", "testKey2", "testKey3", "testKey4",
                        "testKey5"));
        assertIterableEquals(List.of("testKey5"), result.getMissingItems());
        assertIterableEquals(
                List.of("testKey1", "testKey2", "testKey3", "testKey4"),
                result.getOutdatedItems());
        assertIterableEquals(List.of(), result.getHitItems());

        cache.put("testKey1", 5L, 10);
        jsonStr =
                "[{\"key\":\"testKey1\",\"version\":5,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey4\",\"version\":4,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey2\",\"version\":2,\"validUntil\":5,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        result = cache.makeMCCHit(
                List.of("testKey1", "testKey2", "testKey3", "testKey4",
                        "testKey5"));
        assertIterableEquals(List.of("testKey5"), result.getMissingItems());
        assertIterableEquals(List.of("testKey2", "testKey3", "testKey4"),
                             result.getOutdatedItems());
        assertIterableEquals(List.of("testKey1"), result.getHitItems());

        result = cache.makeMCCHit(
                List.of("testKey1", "testKey2", "testKey3", "testKey4"));
        assertIterableEquals(List.of(), result.getMissingItems());
        assertIterableEquals(List.of("testKey2", "testKey3", "testKey4"),
                             result.getOutdatedItems());
        assertIterableEquals(List.of("testKey1"), result.getHitItems());

        cache.invalidate("testKey2", 6L);
        cache.put("testKey2", 6L, 10);
        jsonStr =
                "[{\"key\":\"testKey2\",\"version\":6,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey1\",\"version\":5,\"validUntil\":9223372036854775807,\"dataSize\":10}," +
                        "{\"key\":\"testKey4\",\"version\":4,\"validUntil\":5,\"dataSize\":10}," +
                        "{\"key\":\"testKey3\",\"version\":3,\"validUntil\":5,\"dataSize\":10}]";
        validateListStructure(cache.getDummyHead(), cache.getDummyTail(),
                              jsonStr);

        result = cache.makeMCCHit(
                List.of("testKey1", "testKey2", "testKey3", "testKey4",
                        "testKey5"));
        assertIterableEquals(List.of("testKey5"), result.getMissingItems());
        assertIterableEquals(List.of("testKey3", "testKey4"),
                             result.getOutdatedItems());
        assertIterableEquals(List.of("testKey1", "testKey2"),
                             result.getHitItems());

        result = cache.makeMCCHit(
                List.of("testKey1", "testKey2", "testKey3", "testKey4"));
        assertIterableEquals(List.of(), result.getMissingItems());
        assertIterableEquals(List.of("testKey3", "testKey4"),
                             result.getOutdatedItems());
        assertIterableEquals(List.of("testKey1", "testKey2"),
                             result.getHitItems());
    }


    private void validateListStructure(ConsistentCache.ItemNode head,
                                       ConsistentCache.ItemNode tail,
                                       String nodesJsonStr)
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        List<ConsistentCache.ItemNode> nodes =
                mapper.readValue(nodesJsonStr, new TypeReference<>() {
                });

        ConsistentCache.ItemNode current = head.next;
        for (ConsistentCache.ItemNode node : nodes) {
            assertEquals(current, node);
            current = current.next;
        }
    }


}
