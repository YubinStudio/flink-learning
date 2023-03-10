package com.yu.datastreamapi.timewindow;

import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WaterMakeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //修改默认水位线间隔为100毫秒
//        env.getConfig().setAutoWatermarkInterval(100);

        //从元素中读取数据
        SingleOutputStreamOperator<Event> watermarks = env.fromElements(
                new Event("Tom", "./cart", 1000L),
                new Event("Tom", "./home", 20000L),
                new Event("Judy", "./home", 3000L),
                new Event("Judy", "./home", 4000L),
                new Event("Martin", "./home", 5000L),
                new Event("Martin", "./cart", 6000L),
                new Event("Martin", "./prod", 7000L)
        )
                //有序流的 watermark 生成
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forMonotonousTimestamps()
//                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                                    @Override
//                                    public long extractTimestamp(Event element, long recordTimestamp) {
//                                        return element.timestamp;
//                                    }
//                                })
//                );
                //无序流的 watermark生成,延迟2秒
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        env.execute();

    }
}
