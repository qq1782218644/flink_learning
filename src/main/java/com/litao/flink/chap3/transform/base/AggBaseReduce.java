package com.litao.flink.chap3.transform.base;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
         * 需要注意的是  : sum,min,max只对指定字段进行聚合,minBy和maxBy则对符合条件的整条数据进行保留 , flink聚合函数会为每个key值保留一个状态,聚合进行累积
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Record> ds = env.addSource(new RecordSource());

        ds.keyBy(record -> record.id)
                .max("timestamp")
                .print();


        env.execute();
    }
}
