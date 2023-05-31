package com.litao.flink.chap3.transform.reduce;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import com.litao.flink.utils.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggBaseReduce {
    public static void main(String[] args) throws Exception {
        // 基于KeyedStream,flink提供了几个简单的聚合
        /*
         * 1.sum
         * 2.min
         * 3.max
         * 4.minBy
         * 5.maxBy
         * 需要注意的是  :
         * min,max,sum 只对指定字段记录状态,其他字段值取第一条
         * minBy maxBy 对min或max的整条数据保留
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_1", 3L, 3),
                new WaterSensor("sensor_1", 4L, 4)
        );


        ds.keyBy(record -> record.id)
                .sum("ts")
                .print("min");
        // keyBy后得到的是KeyedStream,经过规约后,又重新得到了DataStream,所以keyBy和reduce是承兑出现的
        // 规约算子,会为每个key保留一个聚合后的状态,并不断更新,且这些状态永不清除,所以使用规约算子,需要注意key的数据量


        env.execute();
    }
}
