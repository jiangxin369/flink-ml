package org.apache.flink.ml.examples;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class CheckpointExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.enableCheckpointing(500);
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
                                            .connect(variableStreams.<Integer>get(0).broadcast())
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
}
