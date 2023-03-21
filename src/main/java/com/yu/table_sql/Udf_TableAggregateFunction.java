package com.yu.table_sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Udf_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.在创建表的DDL语句语句中直接定义事件时间属性
        String createDDL = "CREATE TABLE clickEventTable ( " +
                " user_name STRING ," +
                " url STRING ," +
                " ts BIGINT , " +
                " et AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000))," +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";

        tableEnv.executeSql(createDDL);

        // 3.注册自定义表聚合函数
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        // 4.调用UDF进行查询转换
        String subQuery = "SELECT user_name,COUNT(1) AS cnt, window_start , window_end " +
                "FROM TABLE (" +
                "  TUMBLE(TABLE clickEventTable,DESCRIPTOR(et) ,INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user_name, window_start , window_end ";

        Table aggTable = tableEnv.sqlQuery(subQuery);

        Table resultTable = aggTable.groupBy($("window_end"))
                .flatAggregate(call("Top2", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));

        // 5.转换成流输出
        tableEnv.toChangelogStream(resultTable).print("resultTable ");


        env.execute();
    }

    //定义一个累加器类型，包含第一和第二
    public static class Top2Accumulator {
        public long first;
        public long second;
    }

    // 自定义表聚合函数
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator top2Accumulator = new Top2Accumulator();
            top2Accumulator.first = Long.MIN_VALUE;
            top2Accumulator.second = Long.MIN_VALUE;
            return top2Accumulator;
        }

        // 自定义一个更新累加器的方法
        public void accumulate(Top2Accumulator accumulator, Long value) {
            if (value > accumulator.first) {
                accumulator.second = accumulator.first;
                accumulator.first = value;
            } else if (value > accumulator.second) {
                accumulator.second = value;
            }
        }

        // 输出结果，当前的Top2
        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Long, Integer>> out) {
            if (accumulator.first != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.first, 1));
            }
            if (accumulator.second != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.second, 2));
            }
        }
    }

}
