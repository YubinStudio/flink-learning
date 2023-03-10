package com.yu.datastreamapi.streamconversion;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        //定义侧输出流标签
        OutputTag<Event> maryTag = new OutputTag<Event>("Mary", Types.POJO(Event.class));
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("Bob") {
        };

        SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                // 将对应标签的侧输出流通过Context输出
                if (value.user.equals("Mary"))
                    ctx.output(maryTag, value);
                else if (value.user.equals("Bob"))
                    ctx.output(bobTag, Tuple3.of(value.user, value.url, value.timestamp));
                else
                    //通过out输出主流
                    out.collect(value);
            }
        });

        processStream.print("Main");
        processStream.getSideOutput(maryTag).print("Mary");
        processStream.getSideOutput(bobTag).print("Bob");

        env.execute();
    }
}
