package com.gw.utils;


import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.KryoSerializable;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisClient implements KryoSerializable {
    public static JedisPool  jedisPool;
    public String host;

    public RedisClient(){
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
    }

    public RedisClient(String host){
        this.host=host;
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        jedisPool = new JedisPool(new GenericObjectPoolConfig(), host, 26779);

    }

    static class CleanWorkThread extends Thread{
        @Override
        public void run() {
            System.out.println("Destroy jedis pool");
            if (null != jedisPool){
                jedisPool.destroy();
                jedisPool = null;
            }
        }
    }

    public Jedis getResource(){
        return jedisPool.getResource();
    }

    public void returnResource(Jedis jedis){
        jedisPool.returnResource(jedis);
    }

    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, host);
    }

    public void read(Kryo kryo, Input input) {
        host=kryo.readObject(input, String.class);
        this.jedisPool =new JedisPool(new GenericObjectPoolConfig(), host) ;
    }
}