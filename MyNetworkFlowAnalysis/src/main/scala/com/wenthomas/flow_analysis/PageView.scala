package com.wenthomas.flow_analysis

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @author Verno
 * @create 2020-04-27 5:24 
 */
object PageView {
    def main(args: Array[String]): Unit = {
        //0，创建一个流处理执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //1，从文件中读取数据
        //todo:读不到文件
        val inputStream = env.readTextFile("E:\\project\\MyUserBehaviorAnalysis\\MyNetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
        val dataStream = inputStream.map(data => {
            val dataArray = data.split(",")
            UserBehavior(
                dataArray(0).toLong,
                dataArray(1).toLong,
                dataArray(2).toInt,
                dataArray(3),
                dataArray(4).toLong)
        })
                .assignAscendingTimestamps(_.timestamp * 1000)

        val aggStream = dataStream.filter(_.behavior == "pv")
                .map(x => ("pv", 1))
                //伪分组
                .keyBy(_._1)
                .timeWindow(Time.seconds(60 * 60))
                .sum(1)

        aggStream.print()

        env.execute("PV Job")
    }
}

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)