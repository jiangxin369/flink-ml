package org.apache.flink.ml.examples;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class CheckpointExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(1000);
        env.setParallelism(1);

        SourceFunction<Integer> rowGenerator =
                new SourceFunction<Integer>() {
                    @Override
                    public final void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 0; i < 10; i++) {
                            ctx.collect(i);
                            Thread.sleep(100);
                        }
                    }

                    @Override
                    public void cancel() {}
                };
        DataStream<Integer> dataStream = env.addSource(rowGenerator, "sourceOp").returns(Types.INT);
        DataStream<Integer> variableStream = env.fromElements(5);

        DataStreamList result =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(variableStream),
                        ReplayableDataStreamList.notReplay(dataStream),
                        IterationConfig.newBuilder().build(),
                        (variableStreams, dataStreams) -> {
                            SingleOutputStreamOperator<Integer> processor =
                                    dataStreams
                                            .<Integer>get(0)
                                            .connect(variableStreams.<Integer>get(0))
                                            .transform(
                                                    "Increment",
                                                    BasicTypeInfo.INT_TYPE_INFO,
                                                    new TwoInputReducePerRoundOperator())
                                            .name("CoProcessor");
                            return new IterationBodyResult(
                                    DataStreamList.of(processor),
                                    DataStreamList.of(
                                            processor.getSideOutput(
                                                    TwoInputReducePerRoundOperator.OUTPUT_TAG)),
                                    processor.flatMap(new TerminateOnMaxIter(5)));
                        });

        DataStream<Integer> passBy =
                result.<Integer>get(0).transform("pass-by", Types.INT, new PassByOperator());

        passBy.addSink(new PrintSinkFunction<>()).name("Sink");

        env.execute();
    }

    private static class PassByOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            System.err.println("Entry PassByOperator#snapshotState");
            super.snapshotState(context);
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            output.collect(element);
        }
    }

    private static class TwoInputReducePerRoundOperator extends AbstractStreamOperator<Integer>
            implements TwoInputStreamOperator<Integer, Integer, Integer>,
                    IterationListener<Integer> {

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
                int epochWatermark, Context context, Collector<Integer> collector)
                throws Exception {
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
}
