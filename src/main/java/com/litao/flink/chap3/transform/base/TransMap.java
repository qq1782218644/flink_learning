package com.litao.flink.chap3.transform.base;

import com.litao.flink.utils.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMap {
    public static void main(String[] args) throws Exception {
        // map : 映射
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> ds = env.fromElements(new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2));

        // 通过lambda表达式
        ds.map(water -> water.id)
                .print("f1");

        // 通过匿名内部类
        ds.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) throws Exception {
                return waterSensor.id;
            }
        }).print("f2");

        // 通过实现类
        ds.map(new MyMap()).print("f3");

        env.execute();
    }

    public static class MyMap implements MapFunction<WaterSensor, String> {

        @Override
        public String map(WaterSensor waterSensor) throws Exception {
            return waterSensor.id;
        }
    }
}
