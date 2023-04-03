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

package org.apache.flink.ml.examples.cogroup;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.commons.collections.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** */
public class DataStreamTest {

    public static void main(String[] args) throws Exception {
        long numRecords = Long.parseLong(args[0]);

        boolean useRocksDB = args.length > 1 && "rocksdb".equalsIgnoreCase(args[1]);

        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().disableCheckpointing();
        if (useRocksDB) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
        }

        Random rand = new Random(0);
        List<Tuple3<Integer, Integer, Double>> trainData = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            trainData.add(Tuple3.of(rand.nextInt(50000), rand.nextInt(200000), rand.nextDouble()));
        }
        Collections.shuffle(trainData);

        long start = System.currentTimeMillis();
        DataStream<Tuple3<Integer, Integer, Double>> data1 =
                env.fromCollection(trainData, Types.TUPLE(Types.INT, Types.INT, Types.DOUBLE));
        DataStream<Tuple3<Integer, Integer, Double>> data2 =
                env.fromCollection(trainData, Types.TUPLE(Types.INT, Types.INT, Types.DOUBLE));

        DataStream<Integer> result =
                data1.coGroup(data2)
                        .where(
                                (KeySelector<Tuple3<Integer, Integer, Double>, Integer>)
                                        tuple -> tuple.f0)
                        .equalTo(
                                (KeySelector<Tuple3<Integer, Integer, Double>, Integer>)
                                        tuple -> tuple.f1)
                        .window(EndOfStreamWindows.get())
                        .apply(
                                new RichCoGroupFunction<
                                        Tuple3<Integer, Integer, Double>,
                                        Tuple3<Integer, Integer, Double>,
                                        Integer>() {

                                    @Override
                                    public void open(Configuration parameters) throws Exception {
                                        super.open(parameters);
                                    }

                                    @Override
                                    public void close() throws Exception {
                                        super.close();
                                    }

                                    @Override
                                    public void coGroup(
                                            Iterable<Tuple3<Integer, Integer, Double>> iterable,
                                            Iterable<Tuple3<Integer, Integer, Double>> iterable1,
                                            Collector<Integer> collector) {}
                                });

        IteratorUtils.toList(result.executeAndCollect());

        long finish = System.currentTimeMillis();
        System.out.println("Cost: " + (finish - start) / 1000.0 + " seconds.");
    }
}
