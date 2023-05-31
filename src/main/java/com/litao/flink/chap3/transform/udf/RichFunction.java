package com.litao.flink.chap3.transform.udf;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunction {
    public static void main(String[] args) throws Exception {
        // 富函数类是DataStreamAPI提供的函数类接口,所有的Flink函数类都有Rich版本,以抽象类出现如 : RichMapFunction,RichReduceFunction等
        // 特点 : 1.获取运行环境上下文 2.拥有生命周期方法(open,close) 其中open和close是对应每个并行子任务调用一次
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 对应的输出结果为四条start,代表了每个并行子任务上会执行一次start (在数据到来前)
        env.setParallelism(4);

        DataStreamSource<Record> ds = env.addSource(new RecordSource());

        ds.map(new RichMapFunction<Record, Record>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                RuntimeContext context = getRuntimeContext();
                System.out.println("start:" + context.getIndexOfThisSubtask());
            }


            @Override
            public Record map(Record record) throws Exception {
                return record;
            }

            @Override
            public void close() throws Exception {
                RuntimeContext context = getRuntimeContext();
                System.out.println("end:" + context.getIndexOfThisSubtask());
            }
        });


        env.execute();

    }
}
