package com.litao.flink.chap4.window;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Record> ds = env.addSource(new RecordSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Record>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Record>() {
            @Override
            public long extractTimestamp(Record record, long l) {
                return record.timestamp;
            }
        }));

        // 窗口增量聚合函数之聚合函数 : IN,OUT,ACC类型可以不同
        ds.keyBy(record -> record.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(20L)))
                .aggregate(new AggregateFunction<Record, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        // 初始化ACC,只会在创建窗口的时候执行一次
                        return Tuple2.of(null, 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(Record record, Tuple2<String, Long> stringLongTuple2) {
                        // 处理的方法,第一个参数是数据,第二个参数是acc
                        return Tuple2.of(record.name, stringLongTuple2.f1 + 1L);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
                        // 将acc返回的方法
                        return stringLongTuple2;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
                        // 窗口之间的合并方法,一般只有会话窗口需要合并,这里不进行演示
                        return null;
                    }
                })
                .print();

        env.execute();
    }
}
