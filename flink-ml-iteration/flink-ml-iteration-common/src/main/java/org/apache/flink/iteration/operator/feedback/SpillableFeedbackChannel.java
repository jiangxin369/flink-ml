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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.MutableObjectIterator;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/** Single producer, single consumer channel. */
public final class SpillableFeedbackChannel<T> implements Closeable {

    /** The key that used to identify this channel. */
    private final SubtaskFeedbackKey<T> key;

    /** The underlying queue used to hold the feedback results. */
    private MpscQueueWithSpill<T> queue;

    /** A single registered consumer. */
    private final AtomicReference<ConsumerTask<T>> consumerRef = new AtomicReference<>();

    SpillableFeedbackChannel(SubtaskFeedbackKey<T> key) {
        this.key = Objects.requireNonNull(key);
    }

    public void initialize(
            IOManager ioManager, long inMemoryBufferSize, TypeSerializer<T> serializer) {
        this.queue = new MpscQueueWithSpill<>(ioManager, inMemoryBufferSize, serializer);
    }

    // --------------------------------------------------------------------------------------------------------------
    // API
    // --------------------------------------------------------------------------------------------------------------

    /** Adds a feedback result to this channel. */
    public void put(T element) {
        if (element instanceof StreamRecord) {
            IterationRecord record = (IterationRecord) ((StreamRecord) element).getValue();
            if (record.getType() == IterationRecord.Type.EPOCH_WATERMARK) {
                System.out.println("Putting element: " + element);
            }
        }
        if (queue.add(element) > 1) {
            return;
        }
        @SuppressWarnings("resource")
        final ConsumerTask<T> consumer = consumerRef.get();
        if (consumer == null) {
            // the queue has become non empty at the first time, yet at the same time the (single)
            // consumer has not yet registered, so there is nothing to do.
            // once the consumer would register a drain would be scheduled for the first time.
            return;
        }
        // the queue was previously empty, and now it is not, therefore we schedule a drain.
        consumer.scheduleDrainAll();
        System.out.println("Schedule Drain All....");
    }

    /**
     * Register a feedback iteration consumer.
     *
     * @param consumer the feedback events consumer.
     * @param executor the executor to schedule feedback consumption on.
     */
    public void registerConsumer(final FeedbackConsumer<T> consumer, Executor executor) {
        Objects.requireNonNull(consumer);

        ConsumerTask<T> consumerTask = new ConsumerTask<>(executor, consumer, queue);

        if (!this.consumerRef.compareAndSet(null, consumerTask)) {
            throw new IllegalStateException(
                    "There can be only a single consumer in a FeedbackChannel.");
        }
        // we must try to drain the underlying queue on registration (by scheduling the
        // consumerTask)
        // because
        // the consumer might be registered after the producer has already started producing data
        // into
        // the feedback channel.

        consumerTask.scheduleDrainAll();
    }

    // --------------------------------------------------------------------------------------------------------------
    // Internal
    // --------------------------------------------------------------------------------------------------------------

    /** Closes this channel. */
    @Override
    public void close() {
        ConsumerTask<T> consumer = consumerRef.getAndSet(null);
        IOUtils.closeQuietly(consumer);
        // remove this channel.
        SpillableFeedbackChannelBroker broker = SpillableFeedbackChannelBroker.get();
        broker.removeChannel(key);
        queue.close();
    }

    private static final class ConsumerTask<T> implements Runnable, Closeable {
        private final Executor executor;
        private final FeedbackConsumer<T> consumer;
        private final MpscQueueWithSpill<T> queue;

        ConsumerTask(Executor executor, FeedbackConsumer<T> consumer, MpscQueueWithSpill<T> queue) {
            this.executor = Objects.requireNonNull(executor);
            this.consumer = Objects.requireNonNull(consumer);
            this.queue = Objects.requireNonNull(queue);
        }

        void scheduleDrainAll() {
            executor.execute(this);
        }

        @Override
        public void run() {
            final MutableObjectIterator<T> buffer = queue.drainAll();
            try {
                T element;
                while ((element = buffer.next()) != null) {
                    consumer.processFeedback((T) (new StreamRecord<>(element)));
                }
                System.out.println("Exit while looping...");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            queue.resetStandBy();
        }

        @Override
        public void close() {}
    }
}
