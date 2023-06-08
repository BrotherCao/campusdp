package com.hmdp.utils;


import lombok.val;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private static final long BEGIN_TIMESTAMP = 1640995200L;//2022年1月1日0时0分0秒

    private static final int COUNT_BITS = 32;//序列号的位数

    public long nextId(String keyPrefix){//前缀区分不同业务
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowSecond - BEGIN_TIMESTAMP;
        //2.生成序列号
        //2.1获取当天日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2自增长
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);//key不存在自动创建，1

        //3.拼接并返回
        return timeStamp << COUNT_BITS | count;
    }

    public static void main(String[] args){
        LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println("second:" + second);
    }
}
