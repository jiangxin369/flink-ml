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

package org.apache.flink.ml.servable.builder;

import org.apache.flink.ml.servable.TestUtils;
import org.apache.flink.ml.servable.api.DataFrame;
import org.apache.flink.ml.servable.api.Row;
import org.apache.flink.ml.servable.api.TransformerServable;
import org.apache.flink.ml.servable.builder.ExampleServables.SumModelServable;
import org.apache.flink.ml.servable.types.DataTypes;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** Tests the {@link PipelineModelServable}. */
public class PipelineModelServableTest {

    @Test
    public void testTransform() {
        SumModelServable servableA = new SumModelServable().setDelta(10);
        SumModelServable servableB = new SumModelServable().setDelta(20);
        SumModelServable servableC = new SumModelServable().setDelta(30);

        List<TransformerServable<?>> servables = Arrays.asList(servableA, servableB, servableC);

        TransformerServable<?> pipelineModelServable = new PipelineModelServable(servables);

        DataFrame input =
                new DataFrame(
                        Arrays.asList("input"),
                        Arrays.asList(DataTypes.INT),
                        Arrays.asList(
                                new Row(Arrays.asList(1)),
                                new Row(Arrays.asList(2)),
                                new Row(Arrays.asList(3))));

        DataFrame output = pipelineModelServable.transform(input);

        DataFrame expectedOutput =
                new DataFrame(
                        Arrays.asList("input"),
                        Arrays.asList(DataTypes.INT),
                        Arrays.asList(
                                new Row(Arrays.asList(61)),
                                new Row(Arrays.asList(62)),
                                new Row(Arrays.asList(63))));

        TestUtils.compareDataFrame(expectedOutput, output);
    }
}
