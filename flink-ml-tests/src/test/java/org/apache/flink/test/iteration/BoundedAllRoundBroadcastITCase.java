package org.apache.flink.test.iteration;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.iteration.operators.CollectSink;
import org.apache.flink.test.iteration.operators.OutputRecord;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.test.iteration.UnboundedStreamIterationITCase.computeRoundStat;
import static org.apache.flink.test.iteration.UnboundedStreamIterationITCase.createMiniClusterConfiguration;
import static org.apache.flink.test.iteration.UnboundedStreamIterationITCase.verifyResult;
import static org.junit.Assert.assertEquals;

public class BoundedAllRoundBroadcastITCase extends TestLogger {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private MiniCluster miniCluster;

    private SharedReference<BlockingQueue<OutputRecord<Integer>>> result;

    @Before
    public void setup() throws Exception {
        miniCluster = new MiniCluster(createMiniClusterConfiguration(2, 2));
        miniCluster.start();

        result = sharedObjects.add(new LinkedBlockingQueue<>());
    }

    @After
    public void teardown() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test(timeout = 60000)
    public void testSyncVariableAndConstantBoundedIteration() throws Exception {
        JobGraph jobGraph = createBroadcastJobGraph(result);
        miniCluster.executeJobBlocking(jobGraph);

        assertEquals(6, result.get().size());
        Map<Integer, Tuple2<Integer, Integer>> roundsStat =
                computeRoundStat(result.get(), OutputRecord.Event.EPOCH_WATERMARK_INCREMENTED, 5);

        verifyResult(roundsStat, 5, 1, 4 * (0 + 999) * 1000 / 2);
        assertEquals(OutputRecord.Event.TERMINATED, result.get().take().getEvent());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static JobGraph createBroadcastJobGraph(
            SharedReference<BlockingQueue<OutputRecord<Integer>>> result) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        //        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

        DataStream<Integer> variableSource = env.fromElements(0);
        DataStream<Integer> constSource = env.fromElements(0, 1, 2);

        DataStreamList outputs =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(variableSource),
                        ReplayableDataStreamList.notReplay(constSource),
                        IterationConfig.newBuilder().build(),
                        new BroadcastIterationBody());

        outputs.<OutputRecord<Integer>>get(0).addSink(new CollectSink(result));

        return env.getStreamGraph().getJobGraph();
    }

    @Test
    public void testCreateWrappedOperatorConfigWithSideOutput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();

        DataStream<Long> inputStream = env.fromElements(0L, 1000L, 2000L);

        Iterations.iterateBoundedStreamsUntilTermination(
                DataStreamList.of(inputStream),
                ReplayableDataStreamList.notReplay(inputStream),
                IterationConfig.newBuilder().build(),
                new BroadcastIterationBody());
        env.execute();
    }

    private static class BroadcastIterationBody implements IterationBody {

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Long> variables = variableStreams.get(0);
            DataStream<Long> dataStream = dataStreams.get(0);

            DataStream<Integer> terminationCriteria = variables.flatMap(new TerminateOnMaxIter(5));

            DataStream<Long> newDataStream =
                    BroadcastUtils.withBroadcastStream(
                            Collections.singletonList(dataStream),
                            Collections.singletonMap("key", variables),
                            inputList -> {
                                DataStream input = inputList.get(0);
                                return input.map(new PredictOutputFunction(), Types.LONG);
                            });

            return new IterationBodyResult(
                    DataStreamList.of(variables),
                    DataStreamList.of(newDataStream),
                    terminationCriteria);
        }
    }

    private static class PredictOutputFunction extends RichMapFunction<Long, Long> {
        @Override
        public Long map(Long value) throws Exception {
            return value;
        }
    }
}
