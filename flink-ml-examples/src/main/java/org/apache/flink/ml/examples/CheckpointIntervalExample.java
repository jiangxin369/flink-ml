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

package org.apache.flink.ml.examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class CheckpointIntervalExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        SourceFunction<Long> rowGenerator =
                new SourceFunction<Long>() {
                    @Override
                    public final void run(SourceContext<Long> ctx) throws Exception {
                        for (int i = 0; i < 10; i++) {
                            ctx.collect(System.currentTimeMillis());
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {}
                };

        DataStream<Long> dataStream = env.addSource(rowGenerator, "sourceOp").returns(Types.LONG);

        DataStream<Long> passBy = dataStream.transform("pass-by", Types.LONG, new PassByOperator());

        passBy.addSink(new PrintSinkFunction<>()).name("Sink");

        env.execute();
    }

    private static class PassByOperator extends AbstractStreamOperator<Long>
            implements OneInputStreamOperator<Long, Long> {

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            System.err.println(System.currentTimeMillis() + " Entry PassByOperator#snapshotState");
            super.snapshotState(context);
        }

        @Override
        public void processElement(StreamRecord<Long> element) throws Exception {
            output.collect(element);
        }
    }
}
