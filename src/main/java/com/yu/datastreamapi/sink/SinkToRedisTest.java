package com.yu.datastreamapi.sink;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());


        //创建Jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop01")
                .setPort(6379)
                .build();

        //写入Redis
        /**
         *
         * 1 启动redis
         *      ./src/redis-server redis.conf
         * 2 启动程序
         *
         * 3 启动redis客户端查看数据
         *      /src/redis-cli
         *      keys *          --查看所有key
         *      hgetall events  --查看key中的数据
         */
        streamSource.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    //自定义类实现redisMapper接口
    public static class MyRedisMapper implements RedisMapper<Event> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "events");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.user;
        }

        @Override
        public String getValueFromData(Event event) {
            return event.url;
        }
    }

}
