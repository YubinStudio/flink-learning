package com.yu.cep;

import com.yu.pojo.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LoginFailDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.获取数据
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // 2. 定义模式，连续三次登录失败
        Pattern<LoginEvent, LoginEvent> loginEventPattern =
                Pattern.<LoginEvent>begin("first")   //第一次登录失败事件
                        .where(new SimpleCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent value) throws Exception {
                                return value.eventType.equals("fail");
                            }
                        })
//                .times(3).consecutive()     //使用这种方式 可以简化下面的代码；代表严格匹配 连续三次登录失败事件
//                .times(3).allowCombinations()     //使用这种方式 循环模式中的事件指定非确定性宽松近邻条件，表示可以重复使用已经匹配的事件
                        .next("second")             //第二次登录失败事件
                        .where(new SimpleCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent value) throws Exception {
                                return value.eventType.equals("fail");
                            }
                        })
                        .next("third")              //第三次登录失败事件
                        .where(new SimpleCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent value) throws Exception {
                                return value.eventType.equals("fail");
                            }
                        });

        // 3.将模式应用到数据流上，检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(event -> event.userId), loginEventPattern);

        // 4.提取出检测到的复杂事件，进行报警信息处理
        SingleOutputStreamOperator<String> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> mapLogin) throws Exception {
                // 提取复杂事件中三次登录失败事件
                LoginEvent firstFailEvent = mapLogin.get("first").get(0);
                LoginEvent secondFailEvent = mapLogin.get("second").get(0);
                LoginEvent thirdFailEvent = mapLogin.get("third").get(0);
                return firstFailEvent.userId + "  连续三次登录失败! 登录时间: " +
                        +firstFailEvent.timestamp + ", " +
                        +secondFailEvent.timestamp + ", " +
                        +thirdFailEvent.timestamp;
            }
        });

        warningStream.print("warn ");

        env.execute();
    }
}
