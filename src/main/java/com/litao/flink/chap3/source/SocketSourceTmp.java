package com.litao.flink.chap3.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSourceTmp {
    public static void main(String[] args) throws Exception {
        // 从socket文本流中读取,这种方式一般也是用于测试
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 本机 nc -lp 7777 进行测试
        DataStreamSource<String> ds = env.socketTextStream("localhost", 7777);

        ds.print();

        env.execute();
    }
}
