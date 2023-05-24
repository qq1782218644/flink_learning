package com.litao.flink.chap3.transform.base;

import com.litao.flink.utils.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFilter {
    public static void main(String[] args) throws Exception {
        // filter : 过滤,过滤出sensor_1的数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> ds = env.fromElements(new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2));

        // 通过lambda表达式
        ds.filter(water -> "sensor_2".equals(water.id))
                .print("f1");


        env.execute();
    }

}
