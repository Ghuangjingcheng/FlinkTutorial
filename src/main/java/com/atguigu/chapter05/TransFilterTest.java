package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author:hjc
 * @create 2022/8/31 14:06:08
 */

public class TransFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./page", 1000L)
        );

        SingleOutputStreamOperator<Event> filter1 = stream.filter(data -> {
            return "Alice".equals(data.user);
        });

        filter1.print();

        env.execute();
    }
}
