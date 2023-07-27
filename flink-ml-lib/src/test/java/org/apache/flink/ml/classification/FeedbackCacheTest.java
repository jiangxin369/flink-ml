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

package org.apache.flink.ml.classification;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.iteration.typeinfo.IterationRecordSerializer;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

/** */
public class FeedbackCacheTest {

    @Test
    public void testSerializing() throws IOException {

        int value = Integer.MAX_VALUE + 1;
        System.out.println((byte) (value));

        System.out.println((byte) Integer.MIN_VALUE);

        System.out.println(0x7F);

        DataOutputSerializer output = new DataOutputSerializer(256);
        IterationRecord<Integer> record =
                IterationRecord.newEpochWatermark(Integer.MAX_VALUE + 1, "sender");
        IterationRecordSerializer<Integer> serializer =
                new IterationRecordSerializer<>(IntSerializer.INSTANCE);
        serializer.serialize(record, output);

        DataInputDeserializer input = new DataInputDeserializer(output.wrapAsByteBuffer());

        IterationRecord<Integer> deserializedRecord = serializer.deserialize(input);
        System.out.println(deserializedRecord);
    }

    @Test
    public void testSerializing2() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(baos);

        IterationRecord<Integer> record =
                IterationRecord.newEpochWatermark(Integer.MAX_VALUE + 1, "sender");
        IterationRecordSerializer<Integer> serializer =
                new IterationRecordSerializer<>(IntSerializer.INSTANCE);
        serializer.serialize(record, outputViewStreamWrapper);

        DataInputViewStreamWrapper inputViewStreamWrapper =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(baos.toByteArray()));

        IterationRecord<Integer> deserializedRecord =
                serializer.deserialize(inputViewStreamWrapper);
        System.out.println(deserializedRecord);
    }

    @Test
    public void testIteration() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Long> input =
                env.fromCollection(Collections.singletonList(1L)).map(x -> x, Types.LONG);
        DataStream<Long> feedback =
                env.fromCollection(Collections.singletonList(1L)).map(x -> x, Types.LONG);

        DataStreamList resultList =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(feedback),
                        ReplayableDataStreamList.notReplay(input),
                        IterationConfig.newBuilder().build(),
                        new LargeFeedbackIterationBody());

        DataStream<Long> result = resultList.get(0);
        result.addSink(
                new SinkFunction<Long>() {
                    @Override
                    public void invoke(Long value) throws Exception {
                        SinkFunction.super.invoke(value);
                    }
                });

        env.execute();
    }

    private static class LargeFeedbackIterationBody implements IterationBody {
        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Long> feedback = variableStreams.get(0);
            DataStream<Long> input = dataStreams.get(0);
            SingleOutputStreamOperator<Long> newUpdates =
                    feedback.connect(input)
                            .transform("CCLoop", Types.LONG, new LargeFeedbackLoop());

            DataStream<Long> termination = newUpdates.flatMap(new TerminateOnMaxIter(1));

            return new IterationBodyResult(
                    DataStreamList.of(newUpdates), DataStreamList.of(newUpdates), termination);
        }
    }

    private static class LargeFeedbackLoop extends AbstractStreamOperator<Long>
            implements TwoInputStreamOperator<Long, Long, Long>, IterationListener<Long> {

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<Long> collector) {
            for (int i = 0; i < 9000; i++) {
                collector.collect((long) i);
                if (i % 1000 == 0) {
                    System.out.printf("Output %d records.\n", i);
                }
            }
        }

        @Override
        public void onIterationTerminated(Context context, Collector<Long> collector) {}

        @Override
        public void processElement1(StreamRecord<Long> streamRecord) {}

        @Override
        public void processElement2(StreamRecord<Long> streamRecord) {}
    }
}
