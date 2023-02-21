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

package org.apache.flink.ml.servable.classification.logisticregression;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModelParams;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorSerializer;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.servable.DataFrame;
import org.apache.flink.ml.servable.ModelServable;
import org.apache.flink.ml.servable.Row;
import org.apache.flink.ml.type.BasicType;
import org.apache.flink.ml.type.DataType;
import org.apache.flink.ml.type.DataTypes;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtilsV2;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogisticRegressionModelServable
        implements ModelServable<LogisticRegressionModelServable>,
                LogisticRegressionModelParams<LogisticRegressionModelServable> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    private DenseVector coefficient;

    public LogisticRegressionModelServable() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public DataFrame transform(DataFrame input) {
        int featuresColIndex = input.getIndex(getFeaturesCol());

        List<Row> predictionResults = new ArrayList<>();
        for (Row row : input.collect()) {
            DenseVector features = ((Vector) row.get(featuresColIndex)).toDense();
            row.clone().add(predictOneDataPoint(features, coefficient));
        }

        List<String> outputColNames = new ArrayList<>(input.getColumnNames());
        List<DataType> outputDataTypes = new ArrayList<>(input.getDataTypes());

        outputColNames.addAll(Arrays.asList(getPredictionCol(), getRawPredictionCol()));
        outputDataTypes.addAll(Arrays.asList(DataTypes.DOUBLE, DataTypes.VECTOR(BasicType.DOUBLE)));

        return new DataFrame(outputColNames, outputDataTypes, predictionResults);
    }

    public LogisticRegressionModelServable setModelData(InputStream modelData) throws IOException {
        //        byte[] bytes = IOUtils.toByteArray(modelData);
        DataInputViewStreamWrapper dataInputViewStreamWrapper =
                new DataInputViewStreamWrapper(modelData);
        DenseVectorSerializer serializer = new DenseVectorSerializer();
        DenseVector coefficient = serializer.deserialize(dataInputViewStreamWrapper);
        setCoefficient(coefficient);
        return this;
    }

    public static LogisticRegressionModelServable load(String path) throws IOException {
        LogisticRegressionModelServable servable =
                ReadWriteUtilsV2.loadServableParam(path, LogisticRegressionModelServable.class);

        FileSystem fileSystem = FileSystem.get(URI.create(path));
        Path modelDataPath = ReadWriteUtilsV2.getDataPath(path);

        FileStatus[] files = fileSystem.listStatus(modelDataPath);
        Preconditions.checkState(
                files.length == 1,
                "Only one model data file is expected in the directory %s.",
                path);

        try (FSDataInputStream fsDataInputStream = fileSystem.open(files[0].getPath())) {
            DataInputViewStreamWrapper dataInputViewStreamWrapper =
                    new DataInputViewStreamWrapper(fsDataInputStream);
            DenseVectorSerializer serializer = new DenseVectorSerializer();
            DenseVector coefficient = serializer.deserialize(dataInputViewStreamWrapper);
            servable.setCoefficient(coefficient);
        }
        return servable;
    }

    /**
     * The main logic that predicts one input data point.
     *
     * @param feature The input feature.
     * @param coefficient The model parameters.
     * @return The prediction label and the raw probabilities.
     */
    protected static Row predictOneDataPoint(Vector feature, DenseVector coefficient) {
        double dotValue = BLAS.dot(feature, coefficient);
        double prob = 1 - 1.0 / (1.0 + Math.exp(dotValue));
        return new Row(Arrays.asList(dotValue >= 0 ? 1. : 0., Vectors.dense(1 - prob, prob)));
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    public DenseVector getCoefficient() {
        return coefficient;
    }

    public void setCoefficient(DenseVector coefficient) {
        this.coefficient = coefficient;
    }
}
