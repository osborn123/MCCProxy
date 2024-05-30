package org.mccproxy.cache;

import org.mccproxy.ml.RawFeature;

public interface AccessTracker {
    void recordRead(long timeStep);

    void recordWrite(long timeStep);

    void syncTimeStep(long timeStep);

    RawFeature toRawFeature();

    //    int getNumReadsLastKTimeSteps(int k);
    //
    //    int getNumWritesLastKTimeSteps(int k);
    //
    //    int[] getNumStepsBetweenReads(int k);
    //
    //    int[] getNumStepsBetweenWrites(int k);
}