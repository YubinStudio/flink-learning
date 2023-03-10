package com.yu.datastreamapi.datasource.custom;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 实现并行的ParallelSourceFunction
 */
public class ParallelClickSource implements ParallelSourceFunction<Integer> {
    //作为控制数据生成的标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        Random random = new Random();
        while (running) {
            ctx.collect(random.nextInt());
        }
        Thread.sleep(10000);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
