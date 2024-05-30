package org.mccproxy.cache;

import com.google.common.annotations.VisibleForTesting;
import org.mccproxy.ml.RawFeature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VariableSizeAccessTracker implements AccessTracker {
    private List<Long> readAccesses;
    private List<Long> writeAccesses;
    private long lastUpdateTimeStep;
    private int windowSize;

    public VariableSizeAccessTracker(int windowSize) {
        this.windowSize = windowSize;
        this.readAccesses = new ArrayList<>(
                Collections.nCopies((windowSize + 63) / 64, 0L));
        this.writeAccesses = new ArrayList<>(
                Collections.nCopies((windowSize + 63) / 64, 0L));
        this.lastUpdateTimeStep = 0;
    }

    @VisibleForTesting
    static void shiftBits(List<Long> accesses, long shift) {
        int longShift = (int) (shift / 64);
        if (longShift >= accesses.size()) {
            Collections.fill(accesses, 0L);
        } else {
            int bitShift = (int) (shift % 64);
            for (int i = accesses.size() - 1; i >= longShift; i--) {
                long val = accesses.get(i - longShift) << bitShift;
                if (i - longShift - 1 >= 0 && bitShift > 0) {
                    val |= (accesses.get(i - longShift - 1) >>>
                            (64 - bitShift));
                }
                accesses.set(i, val);
            }
            for (int i = 0; i < longShift; i++) {
                accesses.set(i, 0L);
            }
        }
    }

    @Override
    public void recordRead(long timeStep) {
        syncTimeStep(timeStep);
        readAccesses.set(0, readAccesses.getFirst() | 1L);
    }

    @Override
    public void recordWrite(long timeStep) {
        syncTimeStep(timeStep);
        writeAccesses.set(0, writeAccesses.getFirst() | 1L);
    }

    @Override
    public void syncTimeStep(long timeStep) {
        shiftBits(readAccesses, timeStep - lastUpdateTimeStep);
        shiftBits(writeAccesses, timeStep - lastUpdateTimeStep);
        lastUpdateTimeStep = timeStep;
    }

    //    @Override
    //    public int getNumReadsLastKTimeSteps(int k) {
    //        return countBits(readAccesses, k);
    //    }
    //
    //    @Override
    //    public int getNumWritesLastKTimeSteps(int k) {
    //        return countBits(writeAccesses, k);
    //    }

    //    @Override
    //    public int[] getNumStepsBetweenReads(int k) {
    //        return getNumStepsBetweenAccesses(readAccesses, k);
    //    }

    //    @Override
    //    public int[] getNumStepsBetweenWrites(int k) {
    //        return getNumStepsBetweenAccesses(writeAccesses, k);
    //    }

    //    private int[] getNumStepsBetweenAccesses(List<Long> accessArray, int k) {
    //        int[] distances = new int[k];
    //        int lastOnePos = -1;
    //        int i = 0, j = 0;
    //        while (i < k && j < accessArray.size()) {
    //            long mask = accessArray.get(j);
    //            while (mask != 0 && i < k) {
    //                int pos = Long.numberOfTrailingZeros(mask) + j * 64;
    //                distances[i++] = pos - lastOnePos;
    //                lastOnePos = pos;
    //                mask &= mask - 1;
    //            }
    //            j++;
    //        }
    //        return distances;
    //    }

    @Override
    public RawFeature toRawFeature() {
        return RawFeature.newBuilder().addAllReadAccesses(readAccesses)
                .addAllWriteAccesses(writeAccesses).build();
    }

    //    private int countBits(List<Long> accesses, int k) {
    //        int count = 0;
    //        for (int i = accesses.size() - k / 64; i < accesses.size(); i++) {
    //            count += Long.bitCount(accesses.get(i));
    //        }
    //        if (k % 64 != 0) {
    //            count += Long.bitCount(accesses.get(accesses.size() - k / 64 - 1) &
    //                                           ((1L << (k % 64)) - 1));
    //        }
    //        return count;
    //    }
}