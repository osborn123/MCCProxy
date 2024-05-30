package org.mccproxy.cache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class VariableSizeAccessTrackerTest {

    @Test
    void testShiftBits() {
        List<Long> accesses = new ArrayList<>(List.of(0L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 1);
        assertIterableEquals(List.of(0L, 0L, 0L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 1);
        assertIterableEquals(List.of(2L, 0L, 0L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 2);
        assertIterableEquals(List.of(4L, 0L, 0L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 64);
        assertIterableEquals(List.of(0L, 1L, 0L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 65);
        assertIterableEquals(List.of(0L, 2L, 0L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 128);
        assertIterableEquals(List.of(0L, 0L, 1L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 129);
        assertIterableEquals(List.of(0L, 0L, 2L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 192);
        assertIterableEquals(List.of(0L, 0L, 0L, 1L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 193);
        assertIterableEquals(List.of(0L, 0L, 0L, 2L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 256);
        assertIterableEquals(List.of(0L, 0L, 0L, 0L), accesses);

        accesses = new ArrayList<>(List.of(1L, 0L, 0L, 0L));
        VariableSizeAccessTracker.shiftBits(accesses, 257);
        assertIterableEquals(List.of(0L, 0L, 0L, 0L), accesses);
    }
}
