package com.litao.flink.chap3.transform.splitstream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStream {
    public static void main(String[] args) throws Exception {
        // 使用侧输出流对数据流进行分流(将流分为多条流)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6);

        // 使用侧输出流将流分为 : 奇数流和偶数流
        OutputTag<Integer> tag = new OutputTag<Integer>("s1", Types.INT) {
        };
        SingleOutputStreamOperator<Integer> ouDs = ds.process(new IntProcess(tag));
        SideOutputDataStream<Integer> jiDs = ouDs.getSideOutput(tag);
        ouDs.print("偶数流");
        jiDs.print("奇数流");

        env.execute();
    }

    static class IntProcess extends ProcessFunction<Integer, Integer> {
        // tag用于给测输出流打上标签
        private OutputTag<Integer> tag;

        public IntProcess(OutputTag<Integer> tag) {
            this.tag = tag;
        }

        @Override
        public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
            if (value % 2 == 0) {
                out.collect(value);
            } else {
                // 这里可以输出多条测输出流,但是需要不同tag打上标签用于区分
                ctx.output(tag, value);
            }
        }
    }
}
