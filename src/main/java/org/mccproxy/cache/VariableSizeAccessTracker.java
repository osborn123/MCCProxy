package org.mccproxy.cache;

import java.util.Arrays;

public class VariableSizeAccessTracker implements AccessTracker {
    private long[] readAccesses;
    private long[] writeAccesses;
    private long lastUpdateTimeStep;
    private int windowSize;

    public VariableSizeAccessTracker(int windowSize) {
        this.windowSize = windowSize;
        this.readAccesses = new long[(windowSize + 63) / 64];
        this.writeAccesses = new long[(windowSize + 63) / 64];
        this.lastUpdateTimeStep = 0;
    }

    @Override
    public void recordRead(long timeStep) {
        shiftBits(readAccesses, timeStep - lastUpdateTimeStep);
        readAccesses[(windowSize - 1) / 64] |= 1L << ((windowSize - 1) % 64);
        shiftBits(writeAccesses, timeStep - lastUpdateTimeStep);
        lastUpdateTimeStep = timeStep;
    }

    @Override
    public void recordWrite(long timeStep) {
        shiftBits(readAccesses, timeStep - lastUpdateTimeStep);
        shiftBits(writeAccesses, timeStep - lastUpdateTimeStep);
        writeAccesses[(windowSize - 1) / 64] |= 1L << ((windowSize - 1) % 64);
        lastUpdateTimeStep = timeStep;
    }

    @Override
    public void syncTimeStep(long timeStep) {
        shiftBits(readAccesses, timeStep - lastUpdateTimeStep);
        shiftBits(writeAccesses, timeStep - lastUpdateTimeStep);
        lastUpdateTimeStep = timeStep;
    }

    @Override
    public int getNumReadsLastKTimeSteps(int k) {
        return countBits(readAccesses, k);
    }

    @Override
    public int getNumWritesLastKTimeSteps(int k) {
        return countBits(writeAccesses, k);
    }

    @Override
    public int[] getNumStepsBetweenReads(int k) {
        return getNumStepsBetweenAccesses(readAccesses, k);
    }

    @Override
    public int[] getNumStepsBetweenWrites(int k) {
        return getNumStepsBetweenAccesses(writeAccesses, k);
    }

    private int[] getNumStepsBetweenAccesses(long[] accessArray, int k) {
        int[] distances = new int[k];
        int lastOnePos = -1;
        int i = 0, j = 0;
        while (i < k && j < accessArray.length) {
            long mask = accessArray[j];
            while (mask != 0 && i < k) {
                int pos = Long.numberOfTrailingZeros(mask) + j * 64;
                distances[i++] = pos - lastOnePos;
                lastOnePos = pos;
                mask &= mask - 1;
            }
            j++;
        }
        return distances;
    }

    private void shiftBits(long[] accesses, long shift) {
        if (shift >= windowSize) {
            Arrays.fill(accesses, 0);
        } else {
            int longShift = (int) shift / 64;
            int bitShift = (int) shift % 64;
            for (int i = 0; i < accesses.length - longShift; i++) {
                accesses[i] = (accesses[i + longShift] << bitShift) |
                        (accesses[i + longShift + 1] >>> (64 - bitShift));
            }
            for (int i = accesses.length - longShift; i < accesses.length;
                 i++) {
                accesses[i] = 0;
            }
        }
    }

    private int countBits(long[] accesses, int k) {
        int count = 0;
        for (int i = accesses.length - k / 64; i < accesses.length; i++) {
            count += Long.bitCount(accesses[i]);
        }
        if (k % 64 != 0) {
            count += Long.bitCount(accesses[accesses.length - k / 64 - 1] &
                                           ((1L << (k % 64)) - 1));
        }
        return count;
    }
}