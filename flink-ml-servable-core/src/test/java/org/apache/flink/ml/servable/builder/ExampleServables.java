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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.servable.api.DataFrame;
import org.apache.flink.ml.servable.api.Row;
import org.apache.flink.ml.servable.api.TransformerServable;
import org.apache.flink.ml.servable.types.BasicType;
import org.apache.flink.ml.servable.types.ScalarType;
import org.apache.flink.ml.util.FileUtils;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ServableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Defines a few Servable subclasses to be used in unit tests. */
public class ExampleServables {

    /**
     * A TransformerServable subclass that increments every value in the input dataframe by `delta`
     * and outputs the resulting values.
     */
    public static class SumModelServable implements TransformerServable<SumModelServable> {

        private static final String COL_NAME = "input";

        private final Map<Param<?>, Object> paramMap = new HashMap<>();

        private int delta;

        public SumModelServable() {
            ParamUtils.initializeMapWithDefaultValues(paramMap, this);
        }

        @Override
        public DataFrame transform(DataFrame input) {
            List<Row> outputRows = new ArrayList<>();
            for (Row row : input.collect()) {
                assert row.size() == 1;
                int originValue = (Integer) row.get(0);
                outputRows.add(new Row(Arrays.asList(originValue + delta)));
            }
            return new DataFrame(
                    Arrays.asList(COL_NAME),
                    Arrays.asList(new ScalarType(BasicType.INT)),
                    outputRows);
        }

        @Override
        public Map<Param<?>, Object> getParamMap() {
            return paramMap;
        }

        public static SumModelServable load(String path) throws IOException {
            SumModelServable servable =
                    ServableUtils.loadServableParam(path, SumModelServable.class);

            Path modelDataPath = FileUtils.getDataPath(path);
            try (FSDataInputStream fsDataInputStream =
                    ServableUtils.getModelDataInputStream(modelDataPath)) {
                DataInputViewStreamWrapper dataInputViewStreamWrapper =
                        new DataInputViewStreamWrapper(fsDataInputStream);
                int delta = IntSerializer.INSTANCE.deserialize(dataInputViewStreamWrapper);
                servable.setDelta(delta);
            }
            return servable;
        }

        public SumModelServable setDelta(int delta) {
            this.delta = delta;
            return this;
        }
    }
}
