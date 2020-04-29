package com.wenthomas.market_analysis
import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
/**
 * @author Verno
 * @create 2020-04-28 23:37 
 */
/**
 * APP市场推广统计：分渠道统计用户行为
 *
 * 学习目标：使用自定义数据源进行数据输入
 */
object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //1，从自定义数据源输入数据
        val inputStream = env.addSource(new SimulateMarketEventSource()).assignAscendingTimestamps(_.timestamp)

        //2，开窗聚合
        val resultStream = inputStream.filter(_.behavior != "UNINSTALL")
                .keyBy(data => (data.channel, data.behavior))
                .timeWindow(Time.hours(1), Time.seconds(5))
                //自定义全窗口函数用来统计聚合
                .process(new MarketCountByChannel())


        resultStream.print("result with processwindowfunction")

        env.execute("market count by channel job")
    }

}

// 定义输入数据样例类
case class MarketUserBehavior( userId: String, behavior: String, channel: String, timestamp: Long )
// 定义输出统计的样例类
case class MarketCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long )

/**
 * 自定义数据源：实现RichParallelSourceFunction
 */
class SimulateMarketEventSource() extends RichParallelSourceFunction[MarketUserBehavior] {

    //定义是否在运行的标识位
    var running = true

    //定义用户行为和推广渠道的集合
    private val behaviorList = List("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
    private val channelList = List("appstore", "huaweistore", "weibo", "wechat")

    //定义随机数生成器
    private val rand: Random.type = Random


    override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
        //定义一个发出数据的最大量，用于控制测试数据量
        val maxCount = Long.MaxValue
        var count = 0L

        //while循环，不停生成随机数
        while (running & count < maxCount) {
            val id = UUID.randomUUID().toString
            val behavior = behaviorList(rand.nextInt(behaviorList.size))
            val channel = channelList(rand.nextInt(channelList.size))
            val ts = System.currentTimeMillis()
            ctx.collect(MarketUserBehavior(id, behavior, channel, ts))

            count += 1
            Thread.sleep(500)
        }
    }

    override def cancel(): Unit = running = false
}

/**
 * 自定义全窗口函数ProcessWindowFunction：将获取到的窗口内数据进行聚合封装输出
 */
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketCount, (String, String), TimeWindow] {

    override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketCount]): Unit = {
        val windowStart = new Timestamp(context.window.getStart).toString
        val windowEnd = new Timestamp(context.window.getEnd).toString
        val channel = key._1
        val behavior = key._2
        val count = elements.size
        out.collect(MarketCount(windowStart, windowEnd, channel, behavior, count))
    }
}
