/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;


/**
 * 内存管理实现类
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    private final long totalMemory; // 总存储大小
    private final ReentrantLock lock; //因为会有多线程并发分配和回收ByteBuffer，所以使用锁控制并发，保证线程安全
    private final Deque<ByteBuffer> free; // 已申请未使用的空间
    private final int poolableSize; // 每个内存块大小，即 batch.size（关联free）

    /* condition可以通俗的理解为条件队列。当一个线程在调用了await方法以后，直到线程等待的某个条件为真的时候才会被唤醒。这种方式为线程提供了更加简单的等待/通知模式。
     Condition必须要配合锁一起使用，因为对共享状态变量的访问发生在多线程环境下。一个Condition的实例必须与一个Lock绑定，因此Condition一般都是作为Lock的内部实现。 */
    private final Deque<Condition> waiters; // 监视器对象，配合Lock使用

    /* BufferPool内存管理包含2个部分，已用空间+可用空间（未申请空间+已申请未使用空间） 的总和代表BufferPool的总量，
    用totalMemory表示(由buffer.memory配置)；可使用的空间，它又包括两个部分：上半部分代表未申请未使用的部分，
    用availableMemory表示；下半部分代表已经申请但没有使用的部分，用一个ByteBuffer队列(Deque<ByteBuffer>)表示，
    我们称这个队列为free，队列中的ByteBuffer的大小用poolableSize表示(由batch.size配置)。 */
    private long nonPooledAvailableMemory; // 未申请未使用空间
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;
    private boolean closed;

    /**
     * 创建一个BufferPool
     *
     * @param memory        Bufferpool可分配的总大小
     * @param poolableSize  已申请可用ByteBuffer大小(关联free),初始化为批次大小
     * @param metrics       指标
     * @param time          系统时间
     * @param metricGrpName 指标组name
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio", metricGrpName, "The fraction of time an appender waits for space allocation.");
        MetricName totalMetricName = metrics.metricName("bufferpool-wait-time-total", metricGrpName, "The total time an appender waits for space allocation.");
        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName));
        this.closed = false;
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size             The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException     If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *                                  forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");

        ByteBuffer buffer = null;
        this.lock.lock();  // 申请内存是存在多个线程同时执行申请内存操作（客户端发起的请求）

        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }

        try {
            if (size == poolableSize && !this.free.isEmpty()) // 已申请空闲部分是否满足
                return this.free.pollFirst();

            int freeListSize = freeSize() * this.poolableSize; // 已经申请未使用空闲大小
            if (this.nonPooledAvailableMemory + freeListSize >= size) { // 总可用空间(已申请+未申请的) > 申请大小
                freeUp(size);
                this.nonPooledAvailableMemory -= size;
            } else { // 总可用内存不满足
                int accumulated = 0;
                //  为什么会有多个 Condition 呢？因为这里可能很多个线程都在使用生产者发送消息，可能很多个线程都没有足够的内存分配了，都在等待。
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    this.waiters.addLast(moreMemory);

                    while (accumulated < size) {  // 轮训获取足够多的可使用的空间
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            // 使当前线程一直等待，直到它发出信号或中断，或者指定的等待时间过去（返回false指到达等待时间）
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            recordWaitTime(timeNs);
                        }

                        if (this.closed)
                            throw new KafkaException("Producer closed while allocating memory");

                        if (waitingTimeElapsed) {
                            this.metrics.sensor("buffer-exhausted-records").record();
                            throw new BufferExhaustedException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                        }

                        remainingTimeToBlockNs -= timeNs; // 计算剩余可用阻塞时间

                        // 检查我们是否可以从空闲列表中满足这个请求，否则分配内存
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // 没有足够空闲的，需要调整分配空间 ， 如果分配多了，那么只需要得到足够size的空间
                            // 例如： 需要 50 ，释放出来了 80 ，那么只取 其中的 50 。
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // 在循环的过程中，有异常了。 那么已经释放出来的空间，再还回去。
                    this.nonPooledAvailableMemory += accumulated;
                    // 把自己从等待队列中移除并结束
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            try {
                // this.nonPooledAvailableMemory == 0 && this.free.isEmpty() : 池外内存为0 ，并且空闲的byteBuffer 没有了。
                // !this.waiters.isEmpty() ： 等待队列里有线程正在等待
                // 最后发现内存有富余，则唤醒其他线程
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // 最后的最后，一定得解锁。否则就是BUG了
                lock.unlock();
            }
        }

        if (buffer == null)
            return safeAllocateByteBuffer(size);
        else
            return buffer;
    }

    // Protected for testing
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            // 维护BufferPool各数据
            if (error) {
                this.lock.lock();
                try {
                    // 失败了，归还空间给池外内存
                    this.nonPooledAvailableMemory += size;
                    // //有其他在等待的线程的话，唤醒其他线程
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * 释放缓冲区free 直至 nonPooledAvailableMemory 满足size大小
     */
    private void freeUp(int size) {
        //循环把 free 里的 byteBuffer 全捞出来，给 nonPooledAvailableMemory直到满足 size 大小
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 将缓冲区返回到池中。如果它们具有poolable大小，则将它们添加到空闲列表中，否则只需将内存标记为空闲。
     *
     * @param buffer The buffer to return
     * @param size   The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *               since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {

        /*
         * 如果 size == poolableSize , 就放到 free 中
         * 如果 size != poolableSize , 归还到 nonPooledAvailableMemory 中. buffer 对象没有引用。等待GC释放
         * 有等待线程的话，唤醒线程
         */
        lock.lock();
        try {
            // poolableSize->已申请可用ByteBuffer大小(关联free)
            if (size == this.poolableSize && size == buffer.capacity()) { // 如果是完整的buffer，放回到队列里
                buffer.clear();
                this.free.add(buffer);
            } else { // 不是完整的buffer，标记为空闲内存就可以了。
                this.nonPooledAvailableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free listee
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * 获取未分配的内存
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }

    /**
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be deallocated. All allocations
     * awaiting available memory will be notified to abort.
     */
    public void close() {
        this.lock.lock();
        this.closed = true;
        try {
            for (Condition waiter : this.waiters)
                waiter.signal();
        } finally {
            this.lock.unlock();
        }
    }
}
