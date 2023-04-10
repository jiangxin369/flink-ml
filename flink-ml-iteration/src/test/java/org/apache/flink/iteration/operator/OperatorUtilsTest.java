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

package org.apache.flink.iteration.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.operator.allround.AllRoundOperatorWrapper;
import org.apache.flink.iteration.proxy.ProxyKeySelector;
import org.apache.flink.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link OperatorUtils}. */
public class OperatorUtilsTest extends TestLogger {

    @Test
    public void testCreateWrappedOperatorConfig() throws Exception {
        StreamOperatorFactory<IterationRecord<String>> wrapperFactory =
                new WrapperOperatorFactory<>(
                        SimpleOperatorFactory.of(
                                new MockKeyedProcessOperator<>(new MockKeyedProcessFunction<>())),
                        new AllRoundOperatorWrapper<>());

        OperatorID operatorId = new OperatorID();

        new StreamTaskMailboxTestHarnessBuilder<>(
                        OneInputStreamTask::new,
                        new IterationRecordTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO))
                .addInput(
                        new IterationRecordTypeInfo<>(Types.INT),
                        1,
                        new ProxyKeySelector<>(new MockKeySelector()))
                .modifyStreamConfig(
                        streamConfig -> streamConfig.setStateKeySerializer(IntSerializer.INSTANCE))
                .setupOutputForSingletonOperatorChain(wrapperFactory, operatorId)
                .build();
    }

    private static class MockKeyedProcessOperator<K, IN, OUT>
            extends KeyedProcessOperator<K, IN, OUT> implements OneInputStreamOperator<IN, OUT> {

        public MockKeyedProcessOperator(KeyedProcessFunction<K, IN, OUT> function) {
            super(function);
        }

        @Override
        public void open() throws Exception {
            super.open();
            StreamConfig config = getOperatorConfig();
            ClassLoader cl = getClass().getClassLoader();

            assertEquals(IntSerializer.INSTANCE, config.getTypeSerializerIn(0, cl));
            assertEquals(StringSerializer.INSTANCE, config.getTypeSerializerOut(cl));
            assertEquals(IntSerializer.INSTANCE, config.getStateKeySerializer(cl));
            assertTrue(
                    (KeySelector<?, ?>) config.getStatePartitioner(0, cl)
                            instanceof MockKeySelector);
        }
    }

    private static class MockKeyedProcessFunction<K, IN, OUT>
            extends KeyedProcessFunction<K, IN, OUT> {

        @Override
        public void processElement(IN value, Context ctx, Collector<OUT> out) {}
    }

    private static class MockKeySelector implements KeySelector<Integer, Integer> {

        @Override
        public Integer getKey(Integer value) {
            return value;
        }
    }
}
