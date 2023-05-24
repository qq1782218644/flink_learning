package com.litao.flink.chap3.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceTmp {
    public static void main(String[] args) throws Exception {
        // 从文件中读取,可以是hdfs文件,普通文件等
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file")
                .print();

        env.execute();
    }
}
