package com.yu.datastreamapi.timewindow;

import com.yu.pojo.Event;
import com.yu.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 处理迟到数据
 */
public class ProcessLateDataExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取socket文本流
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        Event event = new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                        return event;
                    }
                })
                // TODO 方式一：设置 watermark 延迟时间，2 秒钟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)
                        ).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.print("input");

        OutputTag<Event> outputTag = new OutputTag<Event>("late");

        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // TODO 方式二：允许窗口处理迟到数据，设置 1 分钟的等待时间
                .allowedLateness(Time.seconds(30))
                // TODO 方式三：将最后的迟到数据输出到侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new WindowUrlViewCountExample.UrlViewCountAgg(), new WindowUrlViewCountExample.UrlViewCountResult());

        result.print();
        result.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
