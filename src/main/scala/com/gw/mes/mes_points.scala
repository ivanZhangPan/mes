package com.gw.mes

import com.alibaba.fastjson.JSON
import com.gw.utils.{Constants, RedisClient}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object mes_points {
  /*
  **@处理实时过点包括工位维度
   */
  def process_station(drr:RDD[String], spark:SparkSession, redis_broadcast:org.apache.spark.broadcast.Broadcast[RedisClient]): Array[(String,String,String, String,String, Long)]  ={
    val data_tpp_vehicle_through_points_history=drr.mapPartitions{
      iter=>
        val iter1 = iter
      //  val redisClient = new RedisClient("10.255.67.6")
        val jedis=redis_broadcast.value.getResource()
        val pipe = jedis.pipelined()
        jedis.auth(Constants.redis_pword)
        jedis.select(2)
        var m:List[String] =  Nil

        while(iter.hasNext) {
          val cur = iter.next()
          val cur1 = JSON.parseObject(cur)
          val vin = cur1.getString("vin")
          val create_time = cur1.getString("create_time")
          val day = create_time.substring(0, 10)
          val state = cur1.getString("state")
          val station_id = cur1.getString("station_id")
          val key: String = station_id+"_"+vin + "_" + state + "_" + day
          val r_vin = jedis.exists(key)


          cur1.put("day", day)
          val cur_tmp = cur1.toJSONString

          //如果不存则放入redis去重,有过期时间
          if (!r_vin) {
            m :+=  cur_tmp
            jedis.setex(key, 2*24*60*60, "1")

          } else {

          }

        }

        m.iterator
    }.cache()

    import spark.implicits._

    var result:Array[(String,String,String, String,String, Long)] = null
    if(data_tpp_vehicle_through_points_history.count()>0){
      val dset = spark.createDataset(data_tpp_vehicle_through_points_history)

      val df_tpp_vehicle_through_points_history=spark.read.json(dset)
      df_tpp_vehicle_through_points_history.printSchema()
      df_tpp_vehicle_through_points_history.createOrReplaceTempView("tmp_table1")
      val sql ="select workshop_id, station_id, station_name, day, state,count(state) as num from tmp_table1 group by workshop_id, station_id, station_name, day, state"

      result =spark.sql(sql).map{
        f=>
          val workshop_id = f.getAs[String]("workshop_id")
          val station_id = f.getAs[String]("station_id")
          val station_name = f.getAs[String]("station_name")
          val day = f.getAs[String]("day")
          val state = f.getAs[String]("state")
          val num = f.getAs[Long]("num")
          (workshop_id, station_id,station_name, day, state, num)
      }.collect()

    }

    data_tpp_vehicle_through_points_history.unpersist()
    result
  }
}
