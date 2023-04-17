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

package org.apache.flink.test.iteration;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.compile.DraftExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.iteration.operators.TwoInputReducePerRoundOperator;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BoundedWithEndinputITCase extends TestLogger {

    @Test
    public void testCreateWrappedOperatorConfig() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DataStream<Integer> variableStream =
                env.addSource(new DraftExecutionEnvironment.EmptySource<Integer>() {});
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3);

        DataStreamList result =
                Iterations.iterateUnboundedStreams(
                        DataStreamList.of(variableStream),
                        DataStreamList.of(dataStream),
                        (variableStreams, dataStreams) -> {
                            SingleOutputStreamOperator transformed =
                                    variableStreams
                                            .<Integer>get(0)
                                            .connect(dataStreams.<Integer>get(0))
                                            .transform(
                                                    "Reducer",
                                                    BasicTypeInfo.INT_TYPE_INFO,
                                                    new TwoInputReducePerRoundOperator())
                                            .setParallelism(1);
                            return new IterationBodyResult(
                                    DataStreamList.of(variableStreams.get(0)),
                                    DataStreamList.of(transformed));
                        });
        assertEquals(6, result.get(0).executeAndCollect().next());
    }
}
