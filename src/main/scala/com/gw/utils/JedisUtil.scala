package com.gw.utils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
object JedisUtil {
  private val jedisConf: JedisPoolConfig = new JedisPoolConfig()
  jedisConf.setMaxTotal(5000)
  jedisConf.setMaxWaitMillis(50000)
  jedisConf.setMaxIdle(300)
  jedisConf.setTestOnBorrow(true)
  jedisConf.setTestOnReturn(true)
  jedisConf.setTestWhileIdle(true)
  jedisConf.setMinEvictableIdleTimeMillis(60000l)
  jedisConf.setTimeBetweenEvictionRunsMillis(3000l)
  jedisConf.setNumTestsPerEvictionRun(-1)
  lazy val redis =  new JedisPool(jedisConf, "hadoop01",6379)
    .getResource()
  redis.auth("passwd")

  def get(key: String): String = {
    try {
      redis.get(key)
    } catch {
      case e: Exception => e.printStackTrace()
        null
    }
  }

  def set(key: String, value: String) = {
    try {
      redis.set(key, value)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def hmset(key: String, map: java.util.Map[String, String]): Unit = {
    //    val redis=pool.getResource
    try {
      redis.hmset(key, map)
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }

  def hset(key: String, field: String, value: String): Unit = {
    //    val redis=pool.getResource
    try {
      redis.hset(key, field, value)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def hget(key: String, field: String): String = {
    try {
      redis.hget(key, field)
    }catch {
      case e:Exception => e.printStackTrace()
        null
    }
  }

  def hgetAll(key: String): java.util.Map[String, String] = {
    try {
      redis.hgetAll(key)
    } catch {
      case e: Exception => e.printStackTrace()
        null
    }
  }


}
