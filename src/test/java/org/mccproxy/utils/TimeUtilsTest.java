package org.mccproxy.utils;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeUtilsTest {

    @Test
    public void testConvertSqlTimestampToMillis() {
        long millis = System.currentTimeMillis();
        java.sql.Timestamp timestamp = new java.sql.Timestamp(millis);
        assertEquals(millis, TimeUtils.convertSqlTimestamoToMillis(timestamp));
    }

    @Test
    public void testConvertSqlTimestampToNanos() {
        long millis = System.currentTimeMillis();
        long nanos = millis * 1000000 + 123456;
        java.sql.Timestamp timestamp = new java.sql.Timestamp(millis);
        timestamp.setNanos((int) (nanos % 1000000000));
        assertEquals(nanos, TimeUtils.convertSqlTimestampToNanos(timestamp));
    }

    @Test
    public void testConvertProtoTimestampToNanos() {
        long nanos = System.nanoTime();
        Timestamp protoTimestamp =
                Timestamp.newBuilder().setSeconds(nanos / 1000000000)
                        .setNanos((int) (nanos % 1000000000)).build();
        assertEquals(nanos,
                     TimeUtils.convertProtoTimestampToNanos(protoTimestamp));
    }

    @Test
    public void testConvertProtoTimestampToMillis() {
        long millis = System.currentTimeMillis();
        Timestamp protoTimestamp =
                Timestamp.newBuilder().setSeconds(millis / 1000)
                        .setNanos((int) (millis % 1000) * 1000000).build();
        assertEquals(millis,
                     TimeUtils.convertProtoTimestampToMillis(protoTimestamp));
    }

    @Test
    public void testConvertMillisToProtoTimestamp() {
        long millis = System.currentTimeMillis();
        Timestamp protoTimestamp =
                TimeUtils.convertMillisToProtoTimestamp(millis);
        assertEquals(millis / 1000, protoTimestamp.getSeconds());
        assertEquals((millis % 1000) * 1000000, protoTimestamp.getNanos());
    }

    @Test
    public void testConvertNanosToProtoTimestamp() {
        long nanos = System.nanoTime();
        Timestamp protoTimestamp =
                TimeUtils.convertNanosToProtoTimestamp(nanos);
        assertEquals(nanos / 1000000000, protoTimestamp.getSeconds());
        assertEquals((int) (nanos % 1000000000), protoTimestamp.getNanos());
    }

    @Test
    public void testConvertSqlTimestampToProtoTimestamp() {
        long millis = System.currentTimeMillis();
        long nanos = millis * 1000000 + 123456;
        java.sql.Timestamp timestamp = new java.sql.Timestamp(millis);
        timestamp.setNanos((int) (nanos % 1000000000));
        Timestamp protoTimestamp = TimeUtils.convertToProtoTimestamp(timestamp);
        assertEquals(millis / 1000, protoTimestamp.getSeconds());
        assertEquals((int) (nanos % 1000000000), protoTimestamp.getNanos());
    }
}
