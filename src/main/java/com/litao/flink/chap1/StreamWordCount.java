package com.litao.flink.chap1;

import com.litao.flink.utils.ConfUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool conf = ConfUtils.getConf();
        // 读取文本流
        DataStreamSource<String> ds = env.socketTextStream(conf.get("hostname"), conf.getInt("port"));

        // word count
        ds.flatMap((String words, Collector<Tuple2<String, Long>> out) -> {
                    for (String word : words.split(",")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))// Types.类型 用于给擦除的泛型重新赋值
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1).print();


        env.execute();
    }
}
