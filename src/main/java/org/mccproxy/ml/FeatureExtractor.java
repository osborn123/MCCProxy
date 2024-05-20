package org.mccproxy.ml;

import org.mccproxy.cache.AccessTracker;

public class FeatureExtractor {
    public FeatureExtractor() {
    }

    public Feature extractFeatures(AccessTracker accessTracker) {
        int readArriveTimesP30 = accessTracker.getNumReadsLastKTimeSteps(30);
        int readArriveTimesP50 = accessTracker.getNumReadsLastKTimeSteps(50);
        int readArriveTimesP100 = accessTracker.getNumReadsLastKTimeSteps(100);
        int readArriveTimesP200 = accessTracker.getNumReadsLastKTimeSteps(200);
        int[] readIntervals = accessTracker.getNumStepsBetweenReads(6);

        int writeArriveTimesP30 = accessTracker.getNumWritesLastKTimeSteps(30);
        int writeArriveTimesP50 = accessTracker.getNumWritesLastKTimeSteps(50);
        int writeArriveTimesP100 =
                accessTracker.getNumWritesLastKTimeSteps(100);
        int writeArriveTimesP200 =
                accessTracker.getNumWritesLastKTimeSteps(200);
        int[] writeIntervals = accessTracker.getNumStepsBetweenWrites(6);

        return Feature.newBuilder().setReadArriveTimesP30(readArriveTimesP30)
                .setReadArriveTimesP50(readArriveTimesP50)
                .setReadArriveTimesP100(readArriveTimesP100)
                .setReadArriveTimesP200(readArriveTimesP200)
                .setReadPDelta0(readIntervals[0])
                .setReadPDelta1(readIntervals[1])
                .setReadPDelta2(readIntervals[2])
                .setReadPDelta3(readIntervals[3])
                .setReadPDelta4(readIntervals[4])
                .setReadPDelta5(readIntervals[5])
                .setWriteArriveTimesP30(writeArriveTimesP30)
                .setWriteArriveTimesP50(writeArriveTimesP50)
                .setWriteArriveTimesP100(writeArriveTimesP100)
                .setWriteArriveTimesP200(writeArriveTimesP200)
                .setWritePDelta0(writeIntervals[0])
                .setWritePDelta1(writeIntervals[1])
                .setWritePDelta2(writeIntervals[2])
                .setWritePDelta3(writeIntervals[3])
                .setWritePDelta4(writeIntervals[4])
                .setWritePDelta5(writeIntervals[5]).build();
    }
}
