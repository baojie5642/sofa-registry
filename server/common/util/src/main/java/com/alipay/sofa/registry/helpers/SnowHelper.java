package com.alipay.sofa.registry.helpers;

import com.sun.net.httpserver.Authenticator;

import java.util.concurrent.atomic.AtomicStampedReference;

// 一种并发的雪花算法
// 如果采用单线程就不会很复杂
// 这个并发的算法有bug,至少性能太低了
// 后面考虑将两个变量封装成一个对象,使用一个版本控制
public class SnowHelper {

    /**
     * Tue Jan 01 00:00:00 CST 2019
     */
    private static final long twepoch = 1546272000000L;
    private static final long sequenceBits = 15L;
    private static final long timestampLeftShift = sequenceBits;
    private static final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private final AtomicStampedReference<Long> lastTimestamp = new AtomicStampedReference<>(-1L, 0);
    private final AtomicStampedReference<Long> sequence = new AtomicStampedReference<>(0L, 0);

    // String.format("Clock moved backwards. Refusing to generate id for %d milliseconds", lastTimestamp- timestamp);

    public final long nextId() {
        int[] stamp = new int[1];
        Long last;
        long time;
        retry0:
        for (; ; ) {
            retry1:
            for (; ; ) {
                last = lastTimestamp.get(stamp);
                if ((time = timeGen()) < last) {
                    if (stamp[0] == lastTimestamp.getStamp()) {
                        throw new RuntimeException();
                    } else {
                        continue retry1;
                    }
                } else {
                    if (stamp[0] == lastTimestamp.getStamp()) {
                        break;
                    } else {
                        continue retry1;
                    }
                }
            }
            retry2:
            for (; ; ) {
                int[] seqStamp = new int[1];
                Long seq;
                if (last.longValue() == time) {
                    retry3:
                    for (; ; ) {
                        seq = sequence.get(seqStamp);
                        long news = (seq + 1) & sequenceMask;
                        int newStamp = seqStamp[0] + 1;
                        if (sequence.compareAndSet(seq, news, seqStamp[0], newStamp)) {
                            if (stamp[0] == lastTimestamp.getStamp()) {
                                break retry3;
                            } else {
                                continue retry0;
                            }
                        }
                    }
                    if (seq.longValue() == 0) {
                        time = untilNextMillis(last.longValue());
                        if (stamp[0] == lastTimestamp.getStamp()) {
                            break retry2;
                        } else {
                            continue retry0;
                        }
                    }
                } else {
                    seq = sequence.get(seqStamp);
                    int newStamp = seqStamp[0] + 1;
                    if (sequence.compareAndSet(seq, 0L, seqStamp[0], newStamp)) {
                        if (stamp[0] == lastTimestamp.getStamp()) {
                            break retry2;
                        } else {
                            continue retry0;
                        }
                    }
                }
            }
            if (lastTimestamp.compareAndSet(last.longValue(), time, stamp[0], stamp[0] + 1)) {
                break retry0;
            } else {
                continue retry0;
            }
        }
        return ((time - twepoch) << timestampLeftShift) | sequence.getReference().longValue();
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
