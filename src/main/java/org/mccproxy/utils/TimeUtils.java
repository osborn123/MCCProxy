package org.mccproxy.utils;

import com.google.protobuf.Timestamp;

public class TimeUtils {

    public static long convertSqlTimestamoToMillis(
            java.sql.Timestamp timestamp) {
        return timestamp.getTime();
    }

    public static long convertSqlTimestampToNanos(
            java.sql.Timestamp timestamp) {
        return timestamp.getTime() * 1000000 + timestamp.getNanos() % 1000000;
    }

    public static long convertProtoTimestampToNanos(Timestamp timestamp) {
        return timestamp.getSeconds() * 1000000000 + timestamp.getNanos();
    }

    public static long convertProtoTimestampToMillis(Timestamp timestamp) {
        return timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000;
    }

    public static Timestamp convertMillisToProtoTimestamp(
            long timeInMilliseconds) {
        long seconds = timeInMilliseconds / 1000;
        int nanos = (int) ((timeInMilliseconds % 1000) * 1000000);
        return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos)
                .build();
    }

    public static Timestamp convertNanosToProtoTimestamp(long nanos) {
        long seconds = nanos / 1000000000;
        int remainingNanos = (int) (nanos % 1000000000);
        return Timestamp.newBuilder().setSeconds(seconds)
                .setNanos(remainingNanos).build();
    }


    public static Timestamp convertToProtoTimestamp(
            java.sql.Timestamp timestamp) {
        return convertNanosToProtoTimestamp(
                convertSqlTimestampToNanos(timestamp));
    }
}
