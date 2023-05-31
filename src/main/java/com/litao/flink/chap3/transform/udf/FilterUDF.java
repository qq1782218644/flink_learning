package com.litao.flink.chap3.transform.udf;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterUDF {
    public static void main(String[] args) throws Exception {
        // 实现MapFunction,FilterFunction,FlatMapFunction对数据进行自定义处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Record> ds = env.addSource(new RecordSource());

        // 实现类的写法,最好将参数通过构造传入,直接在实现类中写死则不够灵活且不够直观
        ds.filter(new NameFilter("James")).print();

        // 通过匿名内部类(或lambda匿名内部类则不需要,因为这两个直观)
        ds.filter(record -> "James".equals(record.name)).print();

        ds.filter(new FilterFunction<Record>() {
            @Override
            public boolean filter(Record record) throws Exception {
                return "James".equals(record.name);
            }
        }).print();


        env.execute();
    }

    static class NameFilter implements FilterFunction<Record> {
        private String filterName;

        public NameFilter(String filterName) {
            this.filterName = filterName;
        }

        @Override
        public boolean filter(Record record) throws Exception {
            // 这里最好不要直接写 "James".equals(record.name) 因为实现类中直接写魔法值不够直观
            return filterName.equals(record.name);
        }
    }
}
