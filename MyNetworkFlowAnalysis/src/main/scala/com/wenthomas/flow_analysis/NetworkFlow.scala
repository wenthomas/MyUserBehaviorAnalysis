package com.wenthomas.flow_analysis
import java.lang
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
 * @author Verno
 * @create 2020-04-27 4:33 
 */
object NetworkFlow {
    def main(args: Array[String]): Unit = {
        //0，创建一个流处理执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //1，从文件中读取数据
        val inputStream = env.readTextFile("E:\\project\\MyUserBehaviorAnalysis\\MyNetworkFlowAnalysis\\src\\main\\resources\\apache.log")
        val dataStream = inputStream.map(line => {
            val lineArray = line.split(" ")
            val simpleDate = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val timestamp = simpleDate.parse(lineArray(3)).getTime
            ApacheLogEvent(lineArray(0), lineArray(2), timestamp, lineArray(5), lineArray(6))
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
                    override def extractTimestamp(element: ApacheLogEvent): Long = {
                        element.eventTime
                    }
                })


        //对于迟到未来得及处理的数据，输出到侧输出流
        val lateOutputTag = new OutputTag[ApacheLogEvent]("late data")
        //2，开窗聚合
        val aggStream = dataStream.filter(data => {
            //正则表达式过滤掉.css、.js等静态资源文件
            val pattern = "^((?!\\/(css|js)$).)*$".r
            //todo
            (pattern findFirstIn data.url).nonEmpty
        })
                .keyBy(_.url)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                //对于乱序数据，允许迟到数据的处理
                .allowedLateness(Time.seconds(60))
                //对于未来得及处理的数据，输出到侧输出流
                .sideOutputLateData(lateOutputTag)
                .aggregate(new CountAgg(), new WindowResultFunction())

        val lateDataStream = aggStream.getSideOutput(lateOutputTag)
        lateDataStream.print("lateDataStream")

        val resultStream = aggStream.keyBy(_.windowEnd)
                .process(new TopNHotUrls(5))

        resultStream.print()

        env.execute("Hot Urls Count Job")
    }
}

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class WindowResultFunction() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
        //input中的数据是CountAgg的输出，只有一个数，所以直接获取迭代器的值即可
        out.collect(UrlViewCount(key, window.getEnd, input.iterator.next))
    }
}

class TopNHotUrls(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    //定义MapState保存所有聚合结果:(url, count)
    private var mapState: MapState[String, Long] = _

    override def open(parameters: Configuration): Unit = {
        mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pagecount-map", classOf[String], classOf[Long]))
    }

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
        mapState.put(value.url, value.count)

        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

        //清除状态用的定时器：
        //注意：由于窗口设定了1min处理迟到数据延迟，则此处应该设定一个相应的延时，以供迟到的数据能继续使用该窗口下的状态进行业务处理。
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        //如果WM触发了清除状态的定时，则清除map中的状态
        //注意：当来到的数据的timestamp到达了windowEnd + 60s，则说明该数据已经过了迟到处理时间，则清空状态，否则维持状态以供迟到数据继续排序输出。
        if (timestamp == ctx.getCurrentKey + 60 * 1000L){
            mapState.clear()
            return
        }

        //从状态中获取数据
        val allUrlViews = new ListBuffer[(String, Long)]()
        val iter = mapState.entries().iterator()
        while(iter.hasNext) {
            val entry = iter.next()
            allUrlViews += ((entry.getKey, entry.getValue))
        }

        val sortedUrlViews = allUrlViews.sortWith((x, y) => x._2 > y._2).take(n)

        //格式化输出显示
        val result = new StringBuilder()
        result.append("时间： ").append(new Timestamp(timestamp - 1)).append("\n")
        for (i <- sortedUrlViews.indices) {
            val currentUrlView = sortedUrlViews(i)
            result.append("No.").append(i + 1).append(":")
                    .append(" URL=").append(currentUrlView._1)
                    .append(" 访问量=").append(currentUrlView._2).append("\n")
        }

        result.append("----------------------------------------------\n\n")

        Thread.sleep(1000)
        out.collect(result.toString())
    }
}