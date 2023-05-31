package com.litao.flink.chap3.transform.unionstream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStream {
    public static void main(String[] args) throws Exception {
        // 数据流的连接操作,只能两条流连接,连接的流可以是不同的类型,连接后的流需要处理成相同的数据类型
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromElements(4, 2, 6, 8, 10);
        DataStreamSource<String> ds2 = env.fromElements("100", "1000", "10000");
        // 步骤一 : 将两条流进行连接,得到ConnectedStreams
        ConnectedStreams<Integer, String> conStream = ds1.connect(ds2);

        // 步骤二 : 对ConnectedStreams进行处理,处理的方式包括 : map , flatmap , process
        SingleOutputStreamOperator<Integer> res = conStream.map(new CoMapFunction<Integer, String, Integer>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                // 对流1进行处理
                return value;
            }

            @Override
            public Integer map2(String value) throws Exception {
                return Integer.valueOf(value);
            }
        });

        // 实现的处理函数分别是 : CoMapFunction,CoFlatMapFunction,CoProcessFunction,实现的思路是对两条流分别处理,处理成相同类型
        res.print();

        env.execute();
    }
}
