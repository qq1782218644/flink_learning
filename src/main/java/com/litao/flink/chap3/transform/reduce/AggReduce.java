package com.litao.flink.chap3.transform.reduce;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Record> ds = env.addSource(new RecordSource());

        // reduce规约需要实现Reduce函数, (A,B)=>C ,注意这三个数据的类型需要相同,A为状态,B为新的数据
        // 实现maxBy("timestamp")的效果
//        ds.keyBy(record -> record.id)
//                .reduce((r1, r2) -> r2.timestamp > r1.timestamp ? r2 : r1)
//                .print();

        // 实现max("timestamp")的效果
        ds.keyBy(record -> record.id)
                .reduce((r1, r2) -> {
                    if (r2.timestamp > r1.timestamp) {
                        r1.timestamp = r2.timestamp;
                    }
                    return r1;
                })
                .print();

        env.execute();
    }
}
