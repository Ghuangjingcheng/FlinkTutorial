package com.atguigu.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author:hjc
 * @create 2022/8/31 10:39:08
 * SourceFunction接口定义的数据源，并行度只能设置为1，如果数据源设置大于1的并行度，则会抛出异常
 */

public class ClickSource implements SourceFunction<Event> {

    // 声明一个布尔变量，作为空置数据生成的标识位
    private Boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        String[] users = {"Marry", "Alice", "Bob", "Cry"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔1秒生成一个点击事件，方便观察
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }


}
