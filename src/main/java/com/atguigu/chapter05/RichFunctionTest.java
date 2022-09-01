package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author:hjc
 * @create 2022/9/1 15:40:09
 */

public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        clicks.map(new RichMapFunction<Event, Long>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + "的任务开始");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + "的任务结束");
            }

            @Override
            public Long map(Event event) throws Exception {
                return event.timestamp;
            }
        }).print();

        env.execute();
    }
}
