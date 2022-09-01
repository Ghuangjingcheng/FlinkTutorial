package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author:hjc
 * @create 2022/9/1 16:22:09
 * shuffle随机分区
 */
public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // shuffle随机分区
//        stream.shuffle().print("shuffle").setParallelism(4);

        // 轮询重分区，Round-Robin负载均衡算法，将输入流数据平均分配到下游的并行任务中
//        stream.rebalance().print("rebalance").setParallelism(4);

        // 重缩放分区
//        stream.rescale().print("rescale").setParallelism(4);

        // 广播,每个分区都会被分发同样的数据
//        stream.broadcast().print("broadcast").setParallelism(4);

        // 全局分区 global
        stream.global().print("global").setParallelism(4);

        env.execute();
    }
}
