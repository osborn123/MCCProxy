package org.mccproxy.cache;

public interface AccessTracker {
    void recordRead(long timeStep);

    void recordWrite(long timeStep);

    void syncTimeStep(long timeStep);

    int getNumReadsLastKTimeSteps(int k);

    int getNumWritesLastKTimeSteps(int k);

    int[] getNumStepsBetweenReads(int k);

    int[] getNumStepsBetweenWrites(int k);
}