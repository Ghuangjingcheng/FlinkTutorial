package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author:hjc
 * @create 2022/8/31 14:02:08
 */

public class TransFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./page", 1000L)
        );

        SingleOutputStreamOperator<String> flatMap1 = stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                collector.collect(event.user);
                collector.collect(event.url);
                collector.collect(event.timestamp.toString());
            }
        }).returns(Types.STRING);

        flatMap1.print();

        env.execute();
    }
}
