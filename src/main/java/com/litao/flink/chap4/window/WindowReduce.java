package com.litao.flink.chap4.window;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Record> ds = env.addSource(new RecordSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Record>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Record>() {
            @Override
            public long extractTimestamp(Record record, long l) {
                return record.timestamp;
            }
        }));

        // 窗口增量聚合函数之规约 : 要求 IN,OUT,ACC类型相同
        ds.keyBy(record -> record.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<Record>() {
                    @Override
                    public Record reduce(Record record, Record t1) throws Exception {
                        // 将page拼接
                        record.page = record.page + t1.page;
                        return record;
                    }
                })
                .print();


        env.execute();
    }
}
