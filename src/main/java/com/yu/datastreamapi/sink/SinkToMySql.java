package com.yu.datastreamapi.sink;

import com.yu.pojo.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToMySql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 10000L),
                new Event("Judy", "./home", 20000L),
                new Event("Martin", "./home", 4000L)
        );

        String sql = "INSERT INTO events(user, url ,timestamp) VALUES (?,?,?)";
        fromElements.addSink(JdbcSink.sink(
                sql,
                ((preparedStatement, event) -> {
                    preparedStatement.setString(1, event.user);
                    preparedStatement.setString(2, event.url);
                    preparedStatement.setString(3, event.timestamp.toString());
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()

        ));


        env.execute();
    }
}
