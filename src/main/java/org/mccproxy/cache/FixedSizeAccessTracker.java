package org.mccproxy.cache;

import org.mccproxy.ml.RawFeature;

public class FixedSizeAccessTracker implements AccessTracker {
    private long readAccesses;
    private long writeAccesses;
    private long lastUpdateTimeStep;

    public FixedSizeAccessTracker() {
        this.readAccesses = 0;
        this.writeAccesses = 0;
        this.lastUpdateTimeStep = 0;
    }

    @Override
    public void recordRead(long timeStep) {
        readAccesses = (readAccesses << (timeStep - lastUpdateTimeStep)) | 1;
        writeAccesses = writeAccesses << (timeStep - lastUpdateTimeStep);
        lastUpdateTimeStep = timeStep;
    }

    @Override
    public void recordWrite(long timeStep) {
        readAccesses = readAccesses << (timeStep - lastUpdateTimeStep);
        writeAccesses = (writeAccesses << (timeStep - lastUpdateTimeStep)) | 1;
        lastUpdateTimeStep = timeStep;
    }

    @Override
    public void syncTimeStep(long timeStep) {
        readAccesses = readAccesses << (timeStep - lastUpdateTimeStep);
        writeAccesses = writeAccesses << (timeStep - lastUpdateTimeStep);
        lastUpdateTimeStep = timeStep;
    }

    @Override
    public RawFeature toRawFeature() {
        return RawFeature.newBuilder().addReadAccesses(readAccesses)
                .addWriteAccesses(writeAccesses).build();
    }
    //
    //    @Override
    //    public int getNumReadsLastKTimeSteps(int k) {
    //        return Long.bitCount(readAccesses & ((1L << k) - 1));
    //    }
    //
    //    @Override
    //    public int getNumWritesLastKTimeSteps(int k) {
    //        return Long.bitCount(writeAccesses & ((1L << k) - 1));
    //    }
    //
    //    @Override
    //    public int[] getNumStepsBetweenReads(int k) {
    //        return getNumStepsBetweenAccesses(readAccesses, k);
    //    }
    //
    //    @Override
    //    public int[] getNumStepsBetweenWrites(int k) {
    //        return getNumStepsBetweenAccesses(writeAccesses, k);
    //    }

    private int[] getNumStepsBetweenAccesses(long accesses, int k) {
        int[] distances = new int[k];
        int lastOnePos = -1;
        int i = 0;
        long mask = accesses;
        while (mask != 0 && i < k) {
            int pos = Long.numberOfTrailingZeros(mask);
            distances[i++] = pos - lastOnePos;
            lastOnePos = pos;
            mask &= mask - 1;
        }
        return distances;
    }
}
