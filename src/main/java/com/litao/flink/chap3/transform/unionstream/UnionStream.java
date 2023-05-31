package com.litao.flink.chap3.transform.unionstream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionStream {
    public static void main(String[] args) throws Exception {
        // 数据流的合流操作,union是讲多条相同数据类型的流合并成一条流,这里 合并的所有流 以及 得到的流的数据类型都相同
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromElements(1, 3, 5, 7, 9);
        DataStreamSource<Integer> ds2 = env.fromElements(4, 2, 6, 8, 10);
        DataStreamSource<String> ds3 = env.fromElements("100", "1000", "10000");

        DataStream<Integer> unionStream = ds1.union(ds2, ds3.map(Integer::valueOf));

        unionStream.print();
        env.execute();
    }
}
