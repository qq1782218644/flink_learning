package com.litao.flink.chap3.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenSource {
    public static void main(String[] args) throws Exception {
        // flink1.17 DataGen连接器新写法,用于随机生成数据进行测试
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource<String> dataGeneratorSource =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>) value -> "Number:" + value,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(10),
                        Types.STRING
                );


        env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagenerator")
                .print();


        env.execute();
    }
}
