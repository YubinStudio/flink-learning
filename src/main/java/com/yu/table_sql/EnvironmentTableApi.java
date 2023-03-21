package com.yu.table_sql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EnvironmentTableApi {
    public static void main(String[] args) {
        StreamExecutionEnvironment env0 = StreamExecutionEnvironment.getExecutionEnvironment();
        env0.setParallelism(1);

        StreamTableEnvironment tableEnv0 = StreamTableEnvironment.create(env0);

        // 1. 定义环境配置来创建表执行环境；
        // 基于blink版本的planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 1.1 基于老版本计划器planner进行流处理（TODO--已弃用）
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();

        TableEnvironment tableEnv1 = TableEnvironment.create(settings);

        // 1.2 基于老版本计划器planner进行批处理（TODO--已弃用）
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);

    }
}
