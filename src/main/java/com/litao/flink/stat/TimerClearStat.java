package com.litao.flink.stat;

import com.litao.flink.utils.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TimerClearStat {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<SensorReading> sensorDs = env
                .fromElements(new SensorReading("001", 1000L, 10L)
                        , new SensorReading("001", 2000L, 20L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading sensorReading, long l) {
                                return sensorReading.ts;
                            }
                        }));

        // 使用KeyProcessFunction对两个连续的温度进行比较,若两者的差值大于一个特定的阈值进行告警
        // 若某个键值超过一个小时的事件时间没有新的温度测量数据则使用计时器进行删除
        sensorDs
                .keyBy(sensor -> sensor.id)
                .process(new CleaningTemperatureAlertFunction(3L))
                .print()
        ;


        env.execute();
    }

    static class CleaningTemperatureAlertFunction extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Long, Long>> {

        // 用户存储最近一次温度的状态
        private ValueState<Long> lastTempState;
        // 用前一个注册的定时器
        private ValueState<Long> lastTimerState;

        private Long threshold;

        public CleaningTemperatureAlertFunction(Long threshold) {
            this.threshold = threshold;
        }

        // 获取状态的句柄
        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态的生存时间 失效时间 = 当前时间 + TTL ,每次操作状态,都会重置生存时间
            // 对于TTL只针对处理时间
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.hours(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 默认也是创建或者写入则重置TTL
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 在失效但是没有被清理的时间内,不可读取(默认)
                    .build();

            ValueStateDescriptor<Long> des1 = new ValueStateDescriptor<>("lastTempState", Types.LONG);
            des1.enableTimeToLive(stateTtlConfig);
            lastTempState = getRuntimeContext().getState(des1);

            ValueStateDescriptor<Long> des2 = new ValueStateDescriptor<Long>("lastTimerState", Types.LONG);
            des2.enableTimeToLive(stateTtlConfig);
            lastTimerState = getRuntimeContext().getState(des2);
        }

        @Override
        public void processElement(SensorReading sensor, KeyedProcessFunction<String, SensorReading, Tuple3<String, Long, Long>>.Context ctx, Collector<Tuple3<String, Long, Long>> out) throws Exception {

            // 设置清理状态的时间为一个小时之后
            Long newTimer = ctx.timestamp() + 3600 * 1000;
            // 获取当前定时器定时的时间
            Long lastTimer = lastTimerState.value();
            // 如果当前定时器不为空则删除
            if (null != lastTimer) {
                // 定时器通过时间戳创建,同时通过时间戳删除
                ctx.timerService().deleteEventTimeTimer(lastTimer);
            }
            // 设定定时器
            ctx.timerService().registerEventTimeTimer(newTimer);
            // 更新定时器时间戳
            lastTimerState.update(newTimer);


            Long lastTemp = lastTempState.value();
            if (null != lastTemp && Math.abs(lastTemp - sensor.temperature) > threshold) {
                // 告警
                out.collect(Tuple3.of(sensor.id, sensor.temperature, Math.abs(sensor.temperature - lastTemp)));
            }

            // 更新lastTemp状态
            lastTempState.update(sensor.temperature);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, Tuple3<String, Long, Long>>.OnTimerContext ctx, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            // 定时器触发则清除所有状态
            lastTimerState.clear();
            lastTempState.clear();
        }
    }
}
