/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.iteration.operator.feedback;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.statefun.flink.core.queue.Lock;
import org.apache.flink.statefun.flink.core.queue.Locks;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Multi producers single consumer fifo queue.
 *
 * <p>This queue supports two operations:
 *
 * <ul>
 *   <li>{@link #add(Object)} atomically adds an element to this queue and returns the number of
 *       elements in the queue after the addition.
 *   <li>{@link #drainAll()} atomically obtains a snapshot of the queue and simultaneously empties
 *       the queue, i.e. drains it.
 * </ul>
 *
 * @param <T> element type
 */
@Internal
public final class MpscQueueWithSpill<T> {
    private final Lock lock = Locks.spinLock();

    private SpillableQueue<T> activeQueue;
    private SpillableQueue<T> standByQueue;

    public MpscQueueWithSpill(
            IOManager ioManager, long inMemoryBufferSize, TypeSerializer<T> serializer) {
        this.activeQueue =
                new SpillableQueue<>(
                        serializer, ioManager, new MemorySegmentPool(inMemoryBufferSize));
        this.standByQueue =
                new SpillableQueue<>(
                        serializer, ioManager, new MemorySegmentPool(inMemoryBufferSize));
    }

    /**
     * Adds an element to this (unbound) queue.
     *
     * @param element the element to add.
     * @return the number of elements in the queue after the addition.
     */
    public long add(T element) {
        Objects.requireNonNull(element);
        final Lock lock = this.lock;
        lock.lockUninterruptibly();

        try {
            SpillableQueue<T> active = this.activeQueue;
            Preconditions.checkState(element instanceof StreamRecord);
            active.add(((StreamRecord<T>) element).getValue());
            return active.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Atomically drains the queue.
     *
     * @return a batch of elements that obtained atomically from that queue.
     */
    public MutableObjectIterator<T> drainAll() {
        final Lock lock = this.lock;
        lock.lockUninterruptibly();
        try {
            System.out.println(
                    "active changed from " + this.activeQueue + " to " + this.standByQueue);
            final SpillableQueue<T> ready = this.activeQueue;
            if (ready.isEmpty()) {
                return EmptyMutableObjectIterator.get();
            }
            // swap active with standby
            this.activeQueue = this.standByQueue;
            this.standByQueue = ready;
            return ready.iterate();
        } finally {
            lock.unlock();
        }
    }

    public void resetStandBy() {
        System.out.println("standby: " + standByQueue + " resetting...");
        standByQueue.reset();
    }

    public void close() {
        try {
            activeQueue.close();
            standByQueue.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
