package com.atguigu.wc;

        import org.apache.flink.api.common.typeinfo.Types;
        import org.apache.flink.api.java.tuple.Tuple;
        import org.apache.flink.api.java.tuple.Tuple2;
        import org.apache.flink.streaming.api.datastream.DataStreamSource;
        import org.apache.flink.streaming.api.datastream.KeyedStream;
        import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.apache.flink.util.Collector;

/**
 * @author:hjc
 * @create 2022/8/24 17:28:08
 */

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环节
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件
        DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");

        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组
        KeyedStream<Tuple2<String, Long>, Tuple> wordAnOneKB = wordAndOne.keyBy(0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAnOneKB.sum(1);

        // 6. 打印
        sum.print();

        // 7.执行
        env.execute();

    }
}
