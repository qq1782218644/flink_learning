package com.litao.flink.chap3.source;

import com.litao.flink.utils.ConfUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String host = ConfUtils.getConf().get("hostname");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", host + ":9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");


        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                "first",
                new SimpleStringSchema(),
                properties
        ));


        stream.print("Kafka");

        env.execute();

    }
}
