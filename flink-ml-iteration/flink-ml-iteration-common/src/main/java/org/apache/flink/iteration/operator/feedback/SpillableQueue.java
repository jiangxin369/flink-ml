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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.io.disk.InputViewIterator;
import org.apache.flink.runtime.io.disk.SpillingBuffer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Objects;

public class SpillableQueue<T> {
    private final DataOutputSerializer output = new DataOutputSerializer(256);

    private TypeSerializer<T> serializer;
    private IOManager ioManager;
    private SpillingBuffer target;

    private long size = 0L;

    public SpillableQueue(
            TypeSerializer<T> serializer,
            IOManager ioManager,
            MemorySegmentPool memorySegmentPool) {
        this.serializer = Objects.requireNonNull(serializer);
        this.ioManager = Objects.requireNonNull(ioManager);

        // SpillingBuffer requires at least 1 memory segment to be present at construction.
        memorySegmentPool.ensureAtLeastOneSegmentPresent();
        this.target =
                new SpillingBuffer(
                        ioManager, memorySegmentPool, memorySegmentPool.getSegmentSize());
    }

    public void add(T item) {
        try {
            output.clear();
            serializer.serialize(item, output);
            target.write(output.getSharedBuffer(), 0, output.length());
            size++;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public long size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public MutableObjectIterator<T> iterate() {
        try {
            DataInputView input = target.flip();
            return new InputViewIterator<>(input, this.serializer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        size = 0;
        try {
            close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MemorySegmentPool pool = new MemorySegmentPool(1_000_000);
        this.target = new SpillingBuffer(ioManager, pool, pool.getSegmentSize());
    }

    public void close() throws IOException {
        output.clear();
        target.close();
    }
}
