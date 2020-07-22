package com.gw.utils

import com.gw.utils.Constants
import redis.clients.jedis.Jedis
/**
  * @Author Do
  * @Date 2020/4/17 22:32
  */
class RedisUtils extends Serializable {
  def getJedisConn: Jedis = {
    new Jedis(Constants.HOST, Constants.PORT, Constants.TIMEOUT)
  }
}
