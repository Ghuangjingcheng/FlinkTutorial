package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author:hjc
 * @create 2022/8/26 16:46:08
 */

public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，设置并行度为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(1);

        // 2.从集合中读取数据
        // 2.1 构建集合
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 1000L));

        DataStreamSource<Event> stream = env.fromCollection(clicks);

        stream.print("1");

        // 2.2 不构建集合
        DataStreamSource<Event> stream2 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 1000L)
        );

        stream2.print("2");

        // 3. 从文件中读取
        DataStreamSource<String> stream3 = env.readTextFile("input/clicks.csv");
        stream3.print("3");

        // 4. 从Socket读取数据
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);
        stream4.print("4");

        env.execute();
    }
}
