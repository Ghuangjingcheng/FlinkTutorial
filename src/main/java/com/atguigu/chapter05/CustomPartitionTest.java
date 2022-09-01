package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author:hjc
 * @create 2022/9/1 16:45:09
 */

public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer integer, int i) {
                        return integer % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer integer) throws Exception {
                        return integer;
                    }
                })
                .print().setParallelism(2);

        env.execute();
    }
}
