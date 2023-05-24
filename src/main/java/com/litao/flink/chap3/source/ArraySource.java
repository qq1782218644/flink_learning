package com.litao.flink.chap3.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class ArraySource {
    public static void main(String[] args) throws Exception {
        // 集合中读取数据源,一般用于测试
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> data = Arrays.asList(1, 3, 10, 22);
        DataStreamSource<Integer> ds = env.fromCollection(data);
        ds.print("array");
        env.execute();
    }
}
