package com.gw.mes

import com.redislabs.provider.redis._
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gw.utils.{Constants, IsTrue, RedisClient, RedisUtils}
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import com.gw.utils.JedisUtil.redis
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Rank


class throughPoints {

}

object throughPoints{
  def main(args: Array[String]): Unit = {
    //创建SparkConf，如果将任务提交到集群中，那么要去掉.setMaster("local[2]")
    val conn: Jedis = new RedisUtils().getJedisConn
    val conf = new SparkConf().setAppName("DirectStream").setMaster("local[2]")
//    conf.set("redis.host", "10.255.67.6")
//    conf.set("redis.port", "26779")  //端口号，不填默认为6379
//    conf.set("redis.auth","123456")  //用户权限配置
//    conf.set("redis.db","1")         //数据库设置
    conf.set("redis.timeout","3000") //设置连接超时时间
    conf.set("spark.streaming.kafka.consumer.poll.ms", "10000")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建一个StreamingContext，其里面包含了一个SparkContext


    val streamingContext = new StreamingContext(conf, Seconds(15))
    val spark = SparkSession .builder().config(conf).getOrCreate()

    import spark.implicits._
    val redisClient = new RedisClient(Constants.redis_host)
   // val jedis= redisClient.getResource()


    val broadcastRedis = streamingContext.sparkContext.broadcast(redisClient)

    //配置kafka的参数

    val kafkaParams = Map[String, Object](

      "bootstrap.servers" -> Constants.bootstrap_servers,

      "key.deserializer" -> classOf[StringDeserializer],

      "value.deserializer" -> classOf[StringDeserializer],

      "group.id" -> "mes_test",

      "auto.offset.reset" -> "latest", // latest

      "enable.auto.commit" -> (false: java.lang.Boolean)

    )


    val topics = Array("topic_start")
    //在Kafka中记录读取偏移量

    val stream = KafkaUtils
      .createDirectStream[String, String](

      streamingContext,

      //位置策略（可用的Executor上均匀分配分区）

      LocationStrategies.PreferConsistent,

      //消费策略（订阅固定的主题集合）

      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)

    )

    //启动
    DBs.setup()
    //迭代DStream中的RDD(KafkaRDD)，将每一个时间点对于的RDD拿出来

    stream.foreachRDD { rdd =>
      if(rdd.isEmpty() == false){
        //获取该RDD对于的偏移量

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


        //拿出对应的数据

        val detail = rdd.map{
          f=>
            val offset = f.offset()

            val partition = f.partition()
            val value = f.value()

            val truejson = IsTrue.isJson(value)

            ((partition, offset), value)

        }


        val tpp_vehicle_through_points_history = detail.filter{case ((partition, offset),value)=>
          val truejson = IsTrue.isJson(value)
          truejson
        }.filter { case ((partition, offset), value) =>
          val jobject:JSONObject = JSON.parseObject(value)
          val data = jobject.get("data")
          val table = jobject.get("table")
          val db = jobject.get("database")

          if(db == "realtime" && table == "tpp_vehicle_through_points_history") true else false
        }.map(f=>f._2)

        var data_tpp_vehicle_through_points_history=tpp_vehicle_through_points_history.map{
          f=>val tpp = JSON.parseObject(f)
            val type1 = tpp.getString("type")
            val data = tpp.getJSONArray("data")
            (type1,data)
        }.flatMap{
          case(type1,f)=> for(i<-0 to f.size()-1) yield {
            val s = f.get(i).asInstanceOf[JSONObject]
            s.put("dbaction", type1)
            s.toString
          }
        }.repartition(1)

        //处理过点工位
        val result_station = mes_points.process_station(data_tpp_vehicle_through_points_history, spark, broadcastRedis)
        println(result_station)

        data_tpp_vehicle_through_points_history=data_tpp_vehicle_through_points_history.mapPartitions{
            iter=>
              val iter1 = iter
              val redisClient = broadcastRedis.value
              val jedis=redisClient.getResource()
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
                val key: String = vin + "_" + state + "_" + day
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

        var result:Array[(String,String, String, Long)] = null
        if(data_tpp_vehicle_through_points_history.count()>0){
          val dset = spark.createDataset(data_tpp_vehicle_through_points_history)

          val df_tpp_vehicle_through_points_history=spark.read.json(dset)

          df_tpp_vehicle_through_points_history.show(20)

          df_tpp_vehicle_through_points_history.createOrReplaceTempView("tmp_table")
          val sql =
            """
              |select workshop_id , day, state,count(state) as num
              |from tmp_table group by workshop_id, day, state
            """.stripMargin

          result = df_tpp_vehicle_through_points_history.sparkSession.sql(sql).map{
            f=>
              val workshop_id = f.getAs[String]("workshop_id")
              val day = f.getAs[String]("day")
              val state = f.getAs[String]("state")
              val num = f.getAs[Long]("num")
              (workshop_id, day, state, num)
          }.collect()

        }

        data_tpp_vehicle_through_points_history.unpersist()


        //设置回滚
        //返回一个事务控制对象
        try{
            DB.localTx { implicit session =>
              //业务数据
              if(result != null)
              for (r <- result) {
                val id = r._1+"_"+r._2+"_"+r._3
                val num = r._4
                SQL("INSERT INTO  ads_mes_line(id,workshop_id,`day`,state,num) VALUES (?, ?,?,?,?) ON DUPLICATE KEY UPDATE num=num+?")
                  .bind(id,r._1, r._2, r._3, num, num) .update().apply()
              }

              if(result_station != null)
                for (r <- result_station) {
                  val id = r._1+"_"+r._2+"_"+r._4+"_"+r._5
                  val num = r._6
                  SQL("INSERT INTO  ads_mes_line(id,workshop_id,station_id,station_name,`day`,state,num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE num=num+?")
                    .bind(id,r._1, r._2, r._3,r._4,r._5, num,num) .update().apply()
                }
            }
            //偏移量
            //SQL
            for(i <- offsetRanges){
              println(i)
              new TopicPartition(i.topic, i.partition)
              val topic_partition = i.topic+"-"+i.partition
              val offset = i.untilOffset
            }

        }catch {
          case _ => println("你报错了，需要回滚")

        }

      }


      //异步更新偏移量到kafka中

      // some time later, after outputs have completed

     // stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)



    }

    streamingContext.start()

    streamingContext.awaitTermination()

  }


}
