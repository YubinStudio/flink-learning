package com.yu.datastreamapi.datasource.custom;

import com.yu.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSourceBounded implements SourceFunction<Event> {
    //作为控制数据生成的标识位
    private Boolean running = true;
    private int size;

    public ClickSourceBounded() {
    }

    public ClickSourceBounded(int size) {
        this.size = size;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        int count = 0;
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running && count < size) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
