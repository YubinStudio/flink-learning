package com.yu.table_sql;

import com.yu.pojo.WeightedAvgAccumulator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class Udf_AggregateFunction {
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

        // 3.注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);

        // 4.调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery(
                "SELECT user_name,WeightedAverage(ts,1) AS w_avg " +
                        "FROM clickEventTable GROUP BY user_name "
        );

        // 5.转换成流输出
        tableEnv.toChangelogStream(resultTable).print("hash ");


        env.execute();
    }

    // 自定义聚合函数, 计算加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {
        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0) {
                return null;
            } else {
                return accumulator.sum / accumulator.count;
            }
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator(0, 0);
        }

        // 累加计算的方法
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }

}
