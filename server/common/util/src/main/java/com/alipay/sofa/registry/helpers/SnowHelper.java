package com.alipay.sofa.registry.helpers;

import java.util.concurrent.atomic.AtomicStampedReference;

// 一种并发的雪花算法
// 如果采用单线程就不会很复杂
public class SnowHelper {

    /** Tue Jan 01 00:00:00 CST 2019 */
    private static final long twepoch            = 1546272000000L;
    private static final long sequenceBits       = 15L;
    private static final long timestampLeftShift = sequenceBits;
    private static final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private final AtomicStampedReference<Long> lastTimestamp=new AtomicStampedReference<>(-1L,0);
    private final AtomicStampedReference<Long> sequence=new AtomicStampedReference<>(0L,0);

    // String.format("Clock moved backwards. Refusing to generate id for %d milliseconds", lastTimestamp- timestamp);

    public final long nextId() {
        long timestamp = timeGen();


        if (timestamp < lastTimestamp) {
            throw new RuntimeException();
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = untilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - twepoch) << timestampLeftShift) | sequence;
    }

    public final long getRealTimestamp(long id) {
        return (id >> timestampLeftShift) + twepoch;
    }

    private final long untilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    private final long timeGen() {
        return System.currentTimeMillis();
    }

}
