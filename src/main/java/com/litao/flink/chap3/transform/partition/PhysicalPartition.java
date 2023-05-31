package com.litao.flink.chap3.transform.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PhysicalPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        env.disableOperatorChaining();

        DataStreamSource<String> ds = env.fromElements("1", "2", "3", "4");
        // 1.Random(完全随机)
        ds.shuffle().print();

        // 2.Round-Robin(轮询发牌,一般用于对数据负载均衡)
        ds.rebalance().print();

        // 3.Rescale(重缩放,类似轮询,但是上游每个对应指定几个下游的分区,轮询是上游对应下游所有分区)
        // 这个有个好处 : 相比于轮询(笛卡尔积),这里减少了网络通讯的负担,效率更高,但是对于负载均衡不一定有效
        ds.rescale().print();

        // 4.Broadcast(广播,将上游每个分区数据发送到下游每个分区,这个操作代价很高,涉及到网络和数据的复制)
        ds.broadcast().print();

        // 5.Global(全局分区,将数据全部合并到下游第一个分区,这个操作需要非常谨慎)
        ds.global().print();

        // 6.Custom(自定义分区) 参数: 第一个是Partitioner,第二个是KeyedSelector
        ds.partitionCustom((o, i) -> 0, value -> value).print();

        env.execute();
    }

}
