package com.yu.datastreamapi.process;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 *      不区分 url 链接，而是将所有访问数据都收集起来，统一
 * 进行统计计算。所以可以不做 keyBy，直接基于 DataStream 开窗，然后使用全窗口函数
 * ProcessAllWindowFunction 来进行处理。
 *      在窗口中可以用一个 HashMap 来保存每个 url 的访问次数，只要遍历窗口中的所有数据，
 * 自然就能得到所有 url 的热门度。最后把 HashMap 转成一个列表 ArrayList，然后进行排序、
 * 取出前两名输出就可以了
 */
public class TopNExampleProcessAllWindowFunction {
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


        //1 .直接使用 map和windowAll开窗，收集所有数据并排序
        stream.map(data -> data.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
                .print();

        env.execute();
    }

    // 实现自定义的增量聚合函数
    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<String, Long>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            // 包含key 计数count + 1
            if (accumulator.containsKey(value)) {
                Long count = accumulator.get(value);
                accumulator.put(value, count + 1L);
            } else {
                //新数据count置为1
                accumulator.put(value, 1L);
            }
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            for (String key : accumulator.keySet()) {
                result.add(Tuple2.of(key, accumulator.get(key)));
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }


    // 实现自定义的全窗口函数，包装输出信息
    public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            StringBuilder result = new StringBuilder();
            result.append("--------------------------");
            result.append("窗口结束时间: " + new Timestamp(context.window().getEnd()) + "\n");


            ArrayList<Tuple2<String, Long>> list = elements.iterator().next();
            // 取List 前两个，包装输出信息
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> currTuple = list.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + currTuple.f0 + " "
                        + "访问量: " + currTuple.f1 + "\n";
                result.append(info);
            }
            result.append("--------------------------\n");
            out.collect(result.toString());
        }
    }

}
