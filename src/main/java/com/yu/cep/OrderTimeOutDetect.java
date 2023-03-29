package com.yu.cep;

import com.yu.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 检测订单超时
 */
public class OrderTimeOutDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.获取订单事件流，并提取时间戳、生成水位线
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.fromElements(
                new OrderEvent("user_1", "order_1", "create", 1000L),
                new OrderEvent("user_2", "order_2", "create", 2000L),
                new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent event, long l) {
                                        return event.timestamp;
                                    }
                                }
                        )
        );

        // 按照订单 ID 分组
//        orderEventStream.keyBy(order -> order.orderId);

        // 2.定义模式
        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")              // 之后是支付事件；中间可以修改订单，宽松近邻
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));      // 15分钟不支付订单超时

        // 3.将模式应用到当前数据流
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(event -> event.orderId), orderEventPattern);

        // 5.将完全匹配和超时匹配的复杂事件提取，进行处理
        SingleOutputStreamOperator<String> result = patternStream.process(new OrderPayMatch());

        result.print("payed  ");
        result.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    // 4.定义静态变量 侧输出流标签
    static OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
    };

    // 自定义PatternProcessFunction
    public static class OrderPayMatch extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {
        // 处理正常匹配事件
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("用户:" + payEvent.userId + " 订单:" + payEvent.orderId + " 已支付！");
        }

        // 处理超时未支付事件
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            OrderEvent createEvent = match.get("create").get(0);
            ctx.output(timeoutTag,"用户:" + createEvent.userId + " 订单:" + createEvent.orderId + " 已超时未支付！");
        }
    }

}
