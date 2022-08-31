package com.atguigu.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author:hjc
 * @create 2022/8/31 17:52:08
 */

public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 3000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Mary", "./page", 2000L),
                new Event("Alice", "./home", 2000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./page", 4000L),
                new Event("Mary", "./home", 5000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Alice", "./page", 4000L)
        );

        // 使用匿名类实现KeySelector
        KeyedStream<Event, String> keyedStream = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });

        // 使用Lambda表达式
        KeyedStream<Event, String> keyedStream1 = stream.keyBy(e -> e.user);

        keyedStream.max("timestamp").print();

        env.execute();
    }
}
