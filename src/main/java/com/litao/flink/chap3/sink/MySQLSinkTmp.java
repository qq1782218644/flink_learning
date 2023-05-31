package com.litao.flink.chap3.sink;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLSinkTmp {
    public static void main(String[] args) throws Exception {
        // 写入到mysql依然需要使用addSink()
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Record> ds = env.addSource(new RecordSource());

        /**
         * TODO 写入mysql
         * 1、只能用老的sink写法： addSink
         * 2、JDBCSink的4个参数:
         *    第一个参数： 执行的sql，一般就是 insert into
         *    第二个参数： 预编译sql， 对占位符填充值
         *    第三个参数： 执行选项 ---》 攒批、重试
         *    第四个参数： 连接选项 ---》 url、用户名、密码
         * 注意 : 写入的表需要有主键
         */

        SinkFunction<Record> mysqlSink = JdbcSink.sink(
                "insert into record (id,name,page,timestamp) values(?,?,?,?)",
                new JdbcStatementBuilder<Record>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Record record) throws SQLException {
                        // 每一条数据使用ps进行填充
                        preparedStatement.setLong(1, record.id);
                        preparedStatement.setString(2, record.name);
                        preparedStatement.setString(3, record.page);
                        preparedStatement.setLong(4, record.timestamp);

                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchIntervalMs(100) // 100一批进行批写
                        .withBatchIntervalMs(3000) // 批次写入时间,若没有攒够,则按照时间写入
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop101:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8&useSSL=false")
                        .withUsername("root")
                        .withPassword("Zs19971224")
                        .withConnectionCheckTimeoutSeconds(60) // 重试超时时间
                        .build()
        );

        ds.addSink(mysqlSink);

        env.execute();
    }
}
