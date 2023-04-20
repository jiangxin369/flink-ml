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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * An operator that reduce the received numbers and emit the result into the output, and also emit
 * the received numbers to the next operator.
 */
public class TwoInputReducePerRoundOperator extends AbstractStreamOperator<Integer>
        implements TwoInputStreamOperator<Integer, Integer, Integer>, IterationListener<Integer> {

    public static final OutputTag<Integer> OUTPUT_TAG = new OutputTag<Integer>("output") {};

    private int inc;

    private List<Integer> nums = new ArrayList<>();

    private ListState<Integer> numState;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        numState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("num", IntSerializer.INSTANCE));
        numState.get().forEach(x -> nums.add(x));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        numState.update(nums);
    }

    @Override
    public void processElement1(StreamRecord<Integer> element) throws Exception {
        nums.add(element.getValue());
    }

    @Override
    public void processElement2(StreamRecord<Integer> element) throws Exception {
        inc = element.getValue();
        System.out.println("processElement2 -------------" + element.getValue());
    }

    @Override
    public void onEpochWatermarkIncremented(
            int epochWatermark, Context context, Collector<Integer> collector) throws Exception {
        System.out.println("Example Epoch is " + epochWatermark);
        collector.collect(inc + 1);
    }

    @Override
    public void onIterationTerminated(Context context, Collector<Integer> collector)
            throws Exception {
        nums.forEach(x -> context.output(OUTPUT_TAG, x + inc));
        numState.clear();
    }
}
