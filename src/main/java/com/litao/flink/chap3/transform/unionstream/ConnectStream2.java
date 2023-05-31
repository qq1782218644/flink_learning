package com.litao.flink.chap3.transform.unionstream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;


public class ConnectStream2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 问题 : connect完全可以使用两条流,map成相同的类型后union,为何还要有connect呢?
        // 答案 : union只是单纯的将流合并起来,而connect可以将流根据指定的key,进行连接
        // 相当于将两条不同的流,按照key统一起来,将key相同的数据数据keyBy到同一个并行子任务,并且可以使用键控状态

        // 说明 1 : 两条DataStream使用connect连接得到ConnectedStream,
        //         然后ConnectedStream可以使用keyBy,将两条流指定的key连接起来,得到ConnectedStream

        // 说明 2 : 这个效果完全等同于两条KeyedStream,直接connect(),得到的ConnectedStream

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        source1.connect(source2).keyBy(s1 -> s1.f0, s2 -> s2.f0).process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("S1:" + value.f0 + ":" + value.toString());
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("S2:" + value.f0 + ":" + value.toString());
            }
        }).print();

        /* 结果发现f0相同的都在同一个分区
            6> S1:1:(1,a1)
            8> S1:2:(2,b)
            6> S2:1:(1,aa1,1)
            8> S2:2:(2,bb,1)
            6> S1:1:(1,a2)
            6> S2:1:(1,aa2,2)
            8> S1:3:(3,c)
            8> S2:3:(3,cc,1)
        * */


        env.execute();
    }
}
