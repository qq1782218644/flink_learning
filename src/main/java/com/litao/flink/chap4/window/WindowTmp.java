package com.litao.flink.chap4.window;

import com.litao.flink.utils.Record;
import com.litao.flink.utils.RecordSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowTmp {
    public static void main(String[] args) throws Exception {
        /*
        键控流的窗口 : 相当于每个key都单独进行了开窗计算操作 stream.keyBy(...).window(...)
        非键控流窗口 : 将所有数据收集到一个并行度上处理 stream.windowAll(...)

        键控流窗口 :

        * 时间窗口:
        ds.keyBy(record -> record.id)
                .window() // 窗口分配器,定义了窗口的类型
                .aggregate() // 窗口函数,定义了触发窗口计算时的逻辑
        窗口分配器 :
        TumblingEventTimeWindows.of(Time.seconds(10)) 10s的事件时间滚动窗口
        SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)) 事件时间滑动窗口,窗口大小10s,步长5s
        EventTimeSessionWindows.withGap(Time.seconds(10)) 事件时间会话窗口,会话过期时长为10s

        TumblingProcessingTimeWindows.of(Time.seconds(10)) 10s的处理时间滚动窗口
        SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)) 处理时间滑动窗口,窗口大小10s,步长5s
        ProcessingTimeSessionWindows.withGap(Time.seconds(10)) 处理时间会话窗口,会话过期时长为10s

        * 计数窗口
        窗口分配器 :
        stream.keyBy(...).countWindow(10) 滚动事件窗口
        stream.keyBy(...).countWindow(10,3) 滚动事件窗口

        * 全局窗口
        全局窗口是计数窗口的底层实现,一般在需要自定义窗口时使用,使用全局窗口,需要自定义窗口触发器才能实现窗口计算
        stream.keyBy(...).window(GlobalWindows.create())

        */

        /*
        窗口函数 : 定义窗口里的计算逻辑
        * 增量聚合函数(相比全窗口函数少了很多信息)
            ** 规约函数 : ReduceFunction 特点 : 窗口中的 IN,OUT,ACC数据类型必须相同
            ** 聚合函数 : AggregateFunction 特点 : 窗口中的 IN,OUT,ACC数据类型可以不同,较为灵活
        * 全窗口函数(相比增量聚合函数性能较差)
            ** 窗口函数 : WindowFunction 特点 : 可以拿到窗口所有数据的可迭代集合,还可以拿到窗口信息,但是建议使用ProcessWindowFunction
            ** 处理窗口函数 : ProcessWindowFunction 特点 : 覆盖了窗口函数特点,同时可以获取到ctx上下文对象(上下文可以获取窗口信息,时间,状态等信息)

        * 增量聚合函数和全窗口函数配置使用(推荐)
        stream.keyBy(...).window(...)
        .aggregate(
            聚合函数,
            全窗口函数
        )

        stream.keyBy(...).window(...)
        .reduce(
            规约函数,
            全窗口函数
        )
        */

        /*
        其他API :
        * 触发器(Trigger)
        主要作用是用来控制窗口什么时候触发计算(执行窗口函数)
        stream.keyBy(...).window(...)
        .trigger(new MyTrigger())

        * 移除器(Evictor)
        stream.keyBy(...).window(...)
        .evictor(new MyEvictor())
        */

    }
}
