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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.Transformer;
import org.apache.flink.ml.classification.logisticregression.LogisticRegression;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModel;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModelData;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.servable.DataFrame;
import org.apache.flink.ml.servable.TransformerServable;
import org.apache.flink.ml.type.BasicType;
import org.apache.flink.ml.type.DataTypes;
import org.apache.flink.ml.util.TestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogisticRegressionModelServableTest extends AbstractTestBase {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private StreamExecutionEnvironment env;

    private StreamTableEnvironment tEnv;

    private static final List<Row> TRAIN_DATA =
            Arrays.asList(
                    Row.of(Vectors.dense(1, 2, 3, 4), 0., 1.),
                    Row.of(Vectors.dense(2, 2, 3, 4), 0., 2.),
                    Row.of(Vectors.dense(3, 2, 3, 4), 0., 3.),
                    Row.of(Vectors.dense(4, 2, 3, 4), 0., 4.),
                    Row.of(Vectors.dense(5, 2, 3, 4), 0., 5.),
                    Row.of(Vectors.dense(11, 2, 3, 4), 1., 1.),
                    Row.of(Vectors.dense(12, 2, 3, 4), 1., 2.),
                    Row.of(Vectors.dense(13, 2, 3, 4), 1., 3.),
                    Row.of(Vectors.dense(14, 2, 3, 4), 1., 4.),
                    Row.of(Vectors.dense(15, 2, 3, 4), 1., 5.));

    private static final DataFrame PREDICT_DATA =
            new DataFrame(
                    Arrays.asList("features", "label", "weight"),
                    Arrays.asList(
                            DataTypes.VECTOR(BasicType.DOUBLE), DataTypes.DOUBLE, DataTypes.DOUBLE),
                    Arrays.asList(
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(1, 2, 3, 4), 0., 1.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(1, 2, 3, 4), 0., 2.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(2, 2, 3, 4), 0., 3.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(3, 2, 3, 4), 0., 4.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(4, 2, 3, 4), 0., 5.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(11, 2, 3, 4), 1., 1.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(12, 2, 3, 4), 1., 2.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(13, 2, 3, 4), 1., 3.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(14, 2, 3, 4), 1., 4.)),
                            new org.apache.flink.ml.servable.Row(
                                    Arrays.asList(Vectors.dense(15, 2, 3, 4), 1., 5.))));

    private static final double[] expectedCoefficient =
            new double[] {0.525, -0.283, -0.425, -0.567};

    private static final double TOLERANCE = 1e-7;

    private Table trainDataTable;

    @Before
    public void before() {
        env = TestUtils.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        Collections.shuffle(TRAIN_DATA);
        trainDataTable =
                tEnv.fromDataStream(
                        env.fromCollection(
                                TRAIN_DATA,
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            DenseVectorTypeInfo.INSTANCE, Types.DOUBLE, Types.DOUBLE
                                        },
                                        new String[] {"features", "label", "weight"})));
    }

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
    public void testLoad() throws Exception {
        LogisticRegression logisticRegression = new LogisticRegression().setWeightCol("weight");
        LogisticRegressionModel model = logisticRegression.fit(trainDataTable);

        LogisticRegressionModelServable servable =
                saveAndLoadServable(tEnv, model, tempFolder.newFolder().getAbsolutePath());

        assertEquals("features", servable.getFeaturesCol());
        assertEquals("prediction", servable.getPredictionCol());
        assertEquals("rawPrediction", servable.getRawPredictionCol());

        assertArrayEquals(expectedCoefficient, servable.getCoefficient().values, 0.1);
    }

    @Test
    public void testTransform() throws Exception {
        LogisticRegression logisticRegression = new LogisticRegression().setWeightCol("weight");
        LogisticRegressionModel model = logisticRegression.fit(trainDataTable);

        LogisticRegressionModelServable servable =
                saveAndLoadServable(tEnv, model, tempFolder.newFolder().getAbsolutePath());

        DataFrame output = servable.transform(PREDICT_DATA);

        verifyPredictionResult(
                output,
                servable.getFeaturesCol(),
                servable.getPredictionCol(),
                servable.getRawPredictionCol());
    }

    @Test
    public void testSaveLoadAndPredict() throws Exception {
        LogisticRegression logisticRegression = new LogisticRegression().setWeightCol("weight");

        LogisticRegressionModel model = logisticRegression.fit(trainDataTable);

        LogisticRegressionModelServable servable =
                saveAndLoadServable(tEnv, model, tempFolder.newFolder().getAbsolutePath());

        System.out.println(servable.getParamMap());
        System.out.println(servable.getCoefficient());
    }

    protected FileSink<LogisticRegressionModelData> createFileSink(String path) {

        return FileSink.forRowFormat(
                        new Path(path), new LogisticRegressionModelData.ModelDataEncoder())
                //                .enableCompact(createFileCompactStrategy(), createFileCompactor())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();
    }

    @Test
    public void testSetModelData() throws Exception {
        String path = tempFolder.newFolder().getAbsolutePath();

        LogisticRegression logisticRegression = new LogisticRegression().setWeightCol("weight");

        LogisticRegressionModel model = logisticRegression.fit(trainDataTable);

        LogisticRegressionModelData.getModelDataStream(model.getModelData()[0])
                .map(x -> x.serialize())
                .sinkTo(createFileSink(path));

        env.execute();
    }

    //    private static FileCompactor createFileCompactor() {
    //        return new RecordWiseFileCompactor<LogisticRegressionModelData>(new
    // DecoderBasedReader.Factory<>(LogisticRegressionModelData.ModelDataDecoder::new));
    //    }
    //
    //    private static FileCompactStrategy createFileCompactStrategy() {
    //        return FileCompactStrategy.Builder.newBuilder().setSizeThreshold(10000).build();
    //    }

    //    @Test
    //    public void testSetModelData() throws Exception {
    //        String path = tempFolder.newFolder().getAbsolutePath();
    //
    //        LogisticRegression logisticRegression = new
    // LogisticRegression().setWeightCol("weight");
    //
    //        LogisticRegressionModel model = logisticRegression.fit(trainDataTable);
    //
    //        LogisticRegressionModelData.getModelDataStream(model.getModelData()[0])
    //                .sinkTo(createFileSink(path));
    //
    //        env.execute();
    //
    //        LogisticRegressionModelServable servable = new LogisticRegressionModelServable();
    //
    //        File file = new File(path);
    //        File[] files = file.listFiles();
    //
    //        try (InputStream inputStream = new FileInputStream(files[0])) {
    //            servable.setModelData(inputStream);
    //        }
    //
    //        assertEquals("features", servable.getFeaturesCol());
    //        assertEquals("prediction", servable.getPredictionCol());
    //        assertEquals("rawPrediction", servable.getRawPredictionCol());
    //
    //        assertArrayEquals(expectedCoefficient, servable.getCoefficient().values, 0.1);
    //    }

    /**
     * Saves a transformer to filesystem and reloads the matadata as a servable by invoking the
     * static loadServable() method.
     */
    public static <S extends TransformerServable<S>, T extends Transformer<T>>
            S saveAndLoadServable(StreamTableEnvironment tEnv, T transformer, String path)
                    throws Exception {
        StreamExecutionEnvironment env = TableUtils.getExecutionEnvironment(tEnv);

        transformer.save(path);
        try {
            env.execute();
        } catch (RuntimeException e) {
            if (!e.getMessage()
                    .equals("No operators defined in streaming topology. Cannot execute.")) {
                throw e;
            }
        }

        Method method = transformer.getClass().getMethod("loadServable", String.class);
        return (S) method.invoke(null, path);
    }

    private void verifyPredictionResult(
            DataFrame output, String featuresCol, String predictionCol, String rawPredictionCol) {
        int featuresColIndex = output.getIndex(featuresCol);
        int predictionColIndex = output.getIndex(predictionCol);
        int rawPredictionColIndex = output.getIndex(rawPredictionCol);

        for (org.apache.flink.ml.servable.Row predictionRow : output.collect()) {
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
