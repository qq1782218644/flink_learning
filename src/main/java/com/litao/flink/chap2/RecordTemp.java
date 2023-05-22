package com.litao.flink.chap2;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RecordTemp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Record> recordDs = env.addSource(new RecordSource());
        recordDs.print();


        env.execute();
    }
}
