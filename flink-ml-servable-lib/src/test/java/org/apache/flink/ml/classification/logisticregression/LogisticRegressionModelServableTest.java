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

package org.apache.flink.ml.classification.logisticregression;

import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.servable.api.DataFrame;
import org.apache.flink.ml.servable.api.Row;
import org.apache.flink.ml.servable.types.BasicType;
import org.apache.flink.ml.servable.types.DataTypes;
import org.apache.flink.ml.util.Serializer;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link LogisticRegressionModelServable}. */
public class LogisticRegressionModelServableTest {

    private static final DataFrame PREDICT_DATA =
            new DataFrame(
                    new ArrayList<>(Arrays.asList("features", "label", "weight")),
                    new ArrayList<>(
                            Arrays.asList(
                                    DataTypes.VECTOR(BasicType.DOUBLE),
                                    DataTypes.DOUBLE,
                                    DataTypes.DOUBLE)),
                    Arrays.asList(
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(1, 2, 3, 4), 0., 1.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(1, 2, 3, 4), 0., 2.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(2, 2, 3, 4), 0., 3.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(3, 2, 3, 4), 0., 4.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(4, 2, 3, 4), 0., 5.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(11, 2, 3, 4), 1., 1.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(12, 2, 3, 4), 1., 2.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(13, 2, 3, 4), 1., 3.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(14, 2, 3, 4), 1., 4.))),
                            new Row(
                                    new ArrayList<>(
                                            Arrays.asList(Vectors.dense(15, 2, 3, 4), 1., 5.)))));

    private static final DenseVector COEFFICIENT = Vectors.dense(0.525, -0.283, -0.425, -0.567);

    private static final double TOLERANCE = 1e-7;

    @Test
    public void testParam() {
        LogisticRegressionModelServable servable = new LogisticRegressionModelServable();
        assertEquals("features", servable.getFeaturesCol());
        assertEquals("prediction", servable.getPredictionCol());
        assertEquals("rawPrediction", servable.getRawPredictionCol());

        servable.setFeaturesCol("test_features")
                .setPredictionCol("test_predictionCol")
                .setRawPredictionCol("test_rawPredictionCol");
        assertEquals("test_features", servable.getFeaturesCol());
        assertEquals("test_predictionCol", servable.getPredictionCol());
        assertEquals("test_rawPredictionCol", servable.getRawPredictionCol());
    }

    @Test
    public void testTransform() {
        LogisticRegressionModelServable servable = new LogisticRegressionModelServable();

        servable.setCoefficient(COEFFICIENT);

        DataFrame output = servable.transform(PREDICT_DATA);

        verifyPredictionResult(
                output,
                servable.getFeaturesCol(),
                servable.getPredictionCol(),
                servable.getRawPredictionCol());
    }

    @Test
    public void testSetModelData() throws IOException {
        byte[] serializedModelData = Serializer.serialize(COEFFICIENT);

        InputStream inputStream = new ByteArrayInputStream(serializedModelData);

        LogisticRegressionModelServable servable = new LogisticRegressionModelServable();

        servable.setModelData(inputStream);

        DataFrame output = servable.transform(PREDICT_DATA);

        verifyPredictionResult(
                output,
                servable.getFeaturesCol(),
                servable.getPredictionCol(),
                servable.getRawPredictionCol());
    }

    private void verifyPredictionResult(
            DataFrame output, String featuresCol, String predictionCol, String rawPredictionCol) {
        int featuresColIndex = output.getIndex(featuresCol);
        int predictionColIndex = output.getIndex(predictionCol);
        int rawPredictionColIndex = output.getIndex(rawPredictionCol);

        for (Row predictionRow : output.collect()) {
            DenseVector feature = ((Vector) predictionRow.get(featuresColIndex)).toDense();
            double prediction = (double) predictionRow.get(predictionColIndex);
            DenseVector rawPrediction = (DenseVector) predictionRow.get(rawPredictionColIndex);
            if (feature.get(0) <= 5) {
                assertEquals(0, prediction, TOLERANCE);
                assertTrue(rawPrediction.get(0) > 0.5);
            } else {
                assertEquals(1, prediction, TOLERANCE);
                assertTrue(rawPrediction.get(0) < 0.5);
            }
        }
    }
}
