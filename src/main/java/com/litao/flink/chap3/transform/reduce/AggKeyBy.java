package com.litao.flink.chap3.transform.reduce;

import com.litao.flink.utils.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // keyBy 是对数据进行hash 取模分区,所以pojo类必须重写hashCode()方法
        // 返回结果是键控流,需要注意  : 并不是转换算子,只是逻辑操作 , keyBy + 之后的操作如sum , 两者是一个算子

        DataStreamSource<WaterSensor> ds = env.fromElements(new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2)
        );

        KeyedStream<WaterSensor, String> ks = ds.keyBy(waterSensor -> waterSensor.id);

        // id相同的全部在同一个分区
        ks.print();

        env.execute();
    }


}
