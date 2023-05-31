package com.litao.flink.chap3.sink;

import com.litao.flink.utils.ConfUtils;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaSinkTmp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 精确一次性写入需要开启ck
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<String> ds = env.addSource(new RecordSource()).map(Object::toString);

        /**
         * Kafka Sink:
         * TODO 注意：如果要使用 精准一次 写入Kafka，需要满足以下条件，缺一不可
         * 1、开启checkpoint
         * 2、设置事务前缀
         * 3、设置事务超时时间：   checkpoint间隔 <  事务超时时间  < max的15分钟
         */

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(ConfUtils.getConf().get("kafka_broker"))
                .setRecordSerializer(
                        // 指定序列化器 , topi
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("test")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 设备一致性级别 : 精确一次 or 至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 精确一次必须设置事务前缀
                .setTransactionalIdPrefix("test-")
                // 精确一次,必须设置事务时间,大于 ck间隔,小于max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        ds.sinkTo(kafkaSink);

        env.execute();
    }
}
