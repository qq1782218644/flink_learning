package com.litao.flink.chap4.window;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Record> ds = env.addSource(new RecordSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Record>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Record>() {
            @Override
            public long extractTimestamp(Record record, long l) {
                return record.timestamp;
            }
        }));

//        ds.keyBy(record -> record.id)
//                .window(TumblingEventTimeWindows.of(Time.seconds(20L)))
//                .process(new ProcessWindowFunction<Record, Tuple2<String, Long>, Long, TimeWindow>() {
//                    @Override
//                    public void process(Long aLong, ProcessWindowFunction<Record, Tuple2<String, Long>, Long, TimeWindow>.Context context, Iterable<Record> elements, Collector<Tuple2<String, Long>> out) throws Exception {
//
//                    }
//                })


        env.execute();
    }
}
