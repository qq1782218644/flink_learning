package com.litao.flink.chap3.transform.base;

import com.litao.flink.utils.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmap {
    public static void main(String[] args) throws Exception {
        // flatmap:flatten + map , 对数据进行扁平化处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // sensor_2的数据输出两遍
        DataStreamSource<WaterSensor> ds = env.fromElements(new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2));

        // 通过lambda表达式
        ds.flatMap((WaterSensor waterSensor, Collector<WaterSensor> collector) -> {
            if ("sensor_2".equals(waterSensor.id)) {
                for (int i = 0; i < 2; i++) {
                    collector.collect(waterSensor);
                }
            }else {
                collector.collect(waterSensor);
            }
        }).returns(Types.POJO(WaterSensor.class)).print("flatmap");


        env.execute();
    }

}
