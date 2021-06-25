package com.alipay.sofa.registry.helpers;

import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IDHelper {

    // 参考  from: https://www.jianshu.com/p/b3c8df455d63
    //

    private static final int ERR = -1234567;

    private final Runtime runtime = Runtime.getRuntime();
    private final int NCPU = runtime.availableProcessors();
    private final AtomicInteger fids = new AtomicInteger(0);
    // 保存future,对任务进行控制,保证数据一致性,同时对资源可控
    private final ConcurrentHashMap<Integer, LocalFuture> futures = new ConcurrentHashMap<>();
    // 存储ids,提供对外部的使用
    private final ConcurrentLinkedQueue<Long> ids = new ConcurrentLinkedQueue<>();
    // ids填满后再次队列上面进行等待被唤醒
    private final LinkedBlockingQueue<byte[]> signals = new LinkedBlockingQueue<>(NCPU * 2);
    // 对ids容量进行大致的控制,不确保准确,但是超出或者少于都可以忽略不计
    private final AtomicInteger curSize = new AtomicInteger(0);

    // 全局的销毁标志
    private final AtomicBoolean stop;
    // ids的大致容量
    private final int contains;
    // ids总容量的三分之一,保证ids少于此值时进行fillAll动作,当多余此值时进行一个一个的添加
    // 后面这个可以设计成比率的形式
    private final int quarter;
    // ids总容量的三分之二,大于此容量时,就不会放里面一个一个的放
    // 这里的总策略可以优化
    private final int quarter2;
    private final ThreadPoolExecutor pool;

    // ==============================Constructors=====================================

    private IDHelper(int size, AtomicBoolean stop) {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }
        if (size < 100000) {
            this.contains = 100000;
        } else if (size > 1000000) {
            this.contains = 1000000;
        } else {
            this.contains = size;
        }
        this.quarter = contains / 3;
        this.quarter2 = 2 * quarter;
        this.stop = stop;
        this.pool = poolBuilder();
        workerBuilder();
        addHook();
    }

    private ThreadPoolExecutor poolBuilder() {
        final ThreadPoolExecutor pool = new ThreadPoolExecutor(NCPU, 2 * NCPU, 180, TimeUnit.SECONDS, new SynchronousQueue<>());
        pool.setThreadFactory(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r, "ids_helper_thread");
                t.setDaemon(true);
                t.setPriority(Thread.NORM_PRIORITY);
                return t;
            }
        });
        return pool;
    }

    private void workerBuilder() {
        for (int i = 0; i < NCPU; i++) {
            buildWorkerOnce();
        }
    }

    private void buildWorkerOnce() {
        int fid = fids.getAndIncrement();
        Worker worker = oneWorker(fid);
        Future future = pool.submit(worker);
        if (future.isDone()) {
            // 可能发生 runtime exception
            // 这个要处理
            // 因为可能放入第一个worker进去的时候
            // 就已经发生系统时间的倒退
            if (!stop.get()) {
                assert false;
            }
        } else {
            LocalFuture lfu = oneFuture(fid, future);
            if (null != futures.putIfAbsent(fid, lfu)) {
                assert false;
            }
        }
    }

    private Worker oneWorker(int fid) {
        return new Worker(fid);
    }

    private LocalFuture oneFuture(int fid, Future future) {
        return new LocalFuture(fid, future);
    }

    private void addHook() {
        runtime.addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutDown();
                runtime.removeShutdownHook(Thread.currentThread());
            }
        }));
    }

    public static final IDHelper create(int size, AtomicBoolean stop) {
        return new IDHelper(size, stop);
    }

    private final class Worker implements Runnable {

        private final int futureId;

        public Worker(int fid) {
            this.futureId = fid;
        }

        @Override
        public void run() {
            for (; ; ) {
                if (stop.get()) {
                    return;
                }
                int sum = curSize.get();
                if (sum < quarter) {
                    sum = fillAll();
                    if (-128 == sum || stop.get()) {
                        return;
                    }
                } else {
                    byte[] sig = waitSignal(300, TimeUnit.MILLISECONDS);
                    if (stop.get()) {
                        return;
                    } else {
                        if (null == sig) {
                            fillOne();
                        } else {
                            fillAll();
                        }
                    }
                }
            }
        }

        private byte[] waitSignal(int timeOut, TimeUnit unit) {
            byte[] sig = null;
            try {
                sig = signals.poll(timeOut, unit);
            } catch (InterruptedException ie) {
                if (stop.get()) {
                    Thread.currentThread().interrupt();
                }
            }
            return sig;
        }

        private int fillAll() {
            int sum = curSize.get();
            for (; ; ) {
                if (stop.get()) {
                    return -128;
                } else {
                    if (sum < contains) {
                        long id = work();
                        if (id == ERR) {
                            replaceWorker();
                        } else {
                            ids.offer(id);
                            sum = curSize.incrementAndGet();
                        }
                    } else {
                        break;
                    }
                }
            }
            return sum;
        }

        private int fillOne() {
            if (curSize.get() < quarter2) {
                long id = work();
                if (id == ERR) {
                    replaceWorker();
                } else {
                    ids.offer(id);
                    curSize.incrementAndGet();
                }
            }
            return curSize.get();
        }

        // 当前线程发现应该抛出异常,所以为自己添加一个后补线程
        private void replaceWorker() {
            if (!stop.get()) {
                LocalFuture lfu = futures.get(getFutureId());
                if (lfu != null && futureId == lfu.getFid()) {
                    lfu.getFuture().cancel(true);
                    pool.purge();
                    futures.remove(getFutureId());
                    buildWorkerOnce();
                    throw new RuntimeException(errors(-1));
                }
            }
        }

        public int getFutureId() {
            return futureId;
        }

    }

    private final class LocalFuture {

        private final int fid;
        private final Future future;

        public LocalFuture(int fid, Future future) {
            this.fid = fid;
            this.future = future;
        }

        public int getFid() {
            return fid;
        }

        public Future getFuture() {
            return future;
        }

    }

    // ==============================Methods==========================================

    public final void shutDown() {
        if (stop.get()) {
            return;
        }
        // 先把标志位设置好
        if (stop.compareAndSet(false, true)) {
            // future id 是自增的,所以可以直接从后向前删除
            int index = Math.max(fids.get(), futures.size());
            for (int i = index; i >= 0; i--) {
                // 然后唤醒阻塞线程
                signals.offer(new byte[0]);
                // 通过 future 取消 worker 的工作
                LocalFuture lfu = futures.remove(i);
                if (null != lfu) {
                    lfu.getFuture().cancel(true);
                }
            }
            pool.purge();
            pool.shutdown();
            futures.clear();
            // 这里没有 clear ids 缓存队列
            // 等待此helper引用消失自行释放
            // 同时也有可能其他线程正在读取缓存数据
        }
    }

    // 此算法中添加一个雪花算法的核心
    private final long work() {
        return -1;
    }

    public final long nextId() {
        Long id = ids.poll();
        if (null != id) {
            curSize.decrementAndGet();
            return id.longValue();
        }
        // 先唤醒工作者,先进行填充
        for (int i = 0; i < NCPU; i++) {
            signals.offer(new byte[0]);
        }
        for (; ; ) {
            if (stop.get()) {
                break;
            }
            id = ids.poll();
            if (null != id) {
                break;
            }
        }
        // 如果跳出循环的同时还未null,说明已经被stop了
        // 直接调用worker返回一个
        if (null == id) {
            return work();
        } else {
            curSize.decrementAndGet();
            return id.longValue();
        }
    }

    private String errors(int time) {
        return String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", time);
    }


}
