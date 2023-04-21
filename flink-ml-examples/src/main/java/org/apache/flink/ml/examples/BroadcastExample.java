package org.apache.flink.ml.examples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.commons.collections.IteratorUtils;

import java.util.Collections;
import java.util.List;

public class BroadcastExample {

    private static final String KEY = "key";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(500);
        env.setParallelism(1);

        DataStream<Integer> source = env.fromElements(100);

        SourceFunction<Integer> rowGenerator =
                new SourceFunction<Integer>() {
                    @Override
                    public final void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 0; i < 10; i++) {
                            ctx.collect(i);
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {}
                };
        DataStream<Integer> broadcast =
                env.addSource(rowGenerator, "sourceOp")
                        .returns(Types.INT)
                        .transform("sleep", Types.INT, new SleepOperator());

        DataStream<Integer> broadcasted =
                BroadcastUtils.withBroadcastStream(
                        Collections.singletonList(source),
                        Collections.singletonMap(KEY, broadcast),
                        inputList -> {
                            DataStream inputData = inputList.get(0);
                            return inputData.map(new AddFunction(), Types.INT);
                        });

        DataStream<Integer> result =
                broadcasted.transform("map_without_sleep", Types.INT, new MapOperator());

        List<Integer> list = IteratorUtils.toList(result.executeAndCollect());
        System.out.println(list);
    }

    private static class AddFunction extends RichMapFunction<Integer, Integer> {

        private List<Integer> nums;

        @Override
        public Integer map(Integer record) throws Exception {
            if (nums == null) {
                nums = getRuntimeContext().getBroadcastVariable(KEY);
            }
            System.out.println("Enter add function: " + nums.get(9));
            return record + nums.get(9);
        }
    }

    private static class SleepOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void processElement(StreamRecord<Integer> streamRecord) throws Exception {
            Thread.sleep(1000);
            output.collect(streamRecord);
            System.out.println("Enter Sleep Operator");
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            System.out.println("Enter snapshot state in Sleep Operator");
        }
    }

    private static class MapOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void processElement(StreamRecord<Integer> streamRecord) throws Exception {

            output.collect(streamRecord);
            System.out.println("Enter Map Operator with record: " + streamRecord.getValue());
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            System.out.println("Enter snapshot state in Map Operator");
        }
    }
}
