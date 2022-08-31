package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author:hjc
 * @create 2022/8/31 11:30:08
 */

public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./page", 1000L)
        );

        // 1.方式一实现MapFunction接口
        SingleOutputStreamOperator<String> map1 = stream.map(new MyMapper1());

        // 2.方式二传入匿名类，实现MapFunction接口
        SingleOutputStreamOperator<String> map2 = stream.map(new MapFunction<Event, String>(){

            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });

        // 3.方式三Lambda表达式
        SingleOutputStreamOperator<String> map3 = stream.map(data -> data.user);

        map3.print("3");

        env.execute();
    }

    private static class MyMapper1 implements MapFunction<Event, String>{
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
