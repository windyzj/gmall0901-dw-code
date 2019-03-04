package com.atguigu.gmall0901.dw.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0901.dw.common.constant.GmallConstant
import com.atguigu.gmall0901.dw.common.util.MyEsUtil
import com.atguigu.gmall0901.dw.realtime.bean.StartupLog
import com.atguigu.gmall0901.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object StartupApp {
  def main(args: Array[String]): Unit = {
         val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall0901-realtime-startup")
         val sc = new SparkContext(sparkConf)

         val ssc = new StreamingContext(sc,Seconds(5))

        val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)
    // 0 验证数据
    //      recordDstream.map(_.value()).foreachRDD(rdd=>
    //          println(rdd.collect().mkString("\n"))
    //        )
    //    1  转成javabean  结构化
    val startupDstream: DStream[StartupLog] = recordDstream.map(_.value()).map { jsonString =>
      val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])
      val formatDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm")
      val logDateTime: String = formatDateTime.format(new Date(startupLog.ts))
      startupLog.logDate  = logDateTime.split(" ")(0)
      startupLog.logHourMinute  = logDateTime.split(" ")(1)
      startupLog.logHour  = logDateTime.split(" ")(1).split(":")(0)
      startupLog
    }
    //    2    过滤今日访问过的用户
//    startupDstream.filter{startupLog=>
//      val jedis:Jedis = RedisUtil.getJedisClient
//      val daukey: String = "dau:" + startupLog.logDate
//      val isExists: Boolean = jedis.sismember(daukey,startupLog.mid)  //判断是否已经存在
//      !isExists   //如果已经存在则不留
//    }


    //    2   过滤今日访问过的用户   利用广播变量
    val filteredStartupLogDstream: DStream[StartupLog] = startupDstream.transform { rdd =>
      println("过滤前共有："+rdd.count())


      //driver
      val jedis: Jedis = RedisUtil.getJedisClient
      val formatDate = new SimpleDateFormat("yyyy-MM-dd")
      val today: String = formatDate.format(new Date())
      val dauSet: util.Set[String] = jedis.smembers("dau:" + today)
      val dauBC: Broadcast[util.Set[String]] = sc.broadcast(dauSet)

      //  executor
      val filteredRDD: RDD[StartupLog] = rdd.filter { startupLog =>
        var isExist = false
        if (dauBC.value != null) {
            isExist  = dauBC.value.contains(startupLog.mid)
        }
        !isExist //如果包含 则过滤掉
      }


      println("过滤后共有："+filteredRDD.count())

      filteredRDD
    }



    //    3     把今日访问用户保存到redis中    daily active user
    //    3.1 redis的存储结构  k-v     key: "dau:"+date    value:  mid

    filteredStartupLogDstream.foreachRDD { rdd =>

        // driver
        rdd.foreachPartition{startupItr=>
            //executor
          val jedis:Jedis = RedisUtil.getJedisClient
          val list = new ListBuffer[Any]()
          for (startupLog <- startupItr ) {
            val daukey: String = "dau:" + startupLog.logDate

            jedis.sadd(daukey,startupLog.mid)
            list+=startupLog
          }
          jedis.close()
          MyEsUtil.insertBulk(GmallConstant.ES_INDEX_DAU,list.toList)

        }

    }

    //目标 es   4 保存es   //利用redis进行去重 ，去重后保存到es

    ssc.start()
    ssc.awaitTermination()
  }
}
