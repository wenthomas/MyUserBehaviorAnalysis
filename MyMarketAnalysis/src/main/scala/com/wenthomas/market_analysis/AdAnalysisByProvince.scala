package com.wenthomas.market_analysis
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-29 1:10 
 */
/**
 * 页面广告点击量统计及黑名单过滤
 *
 * 学习目标：使用processFunction对刷单用户进行黑名单添加过滤，从主流剔除到侧输出流中
 */
object AdAnalysisByProvince {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //1，从文件中读取数据，转换成样例类，并提取时间戳生成WM
        val resource = getClass.getResource("/AdClickLog.csv")
        val inputStream = env.readTextFile(resource.getPath)
        val adLogStream = inputStream.map(data => {
            val dataArray = data.split(",")
            AdClickEvent(
                dataArray(0).toLong,
                dataArray(1).toLong,
                dataArray(2),
                dataArray(3),
                dataArray(4).toLong
            )
        })
                //根据数据源大致估计为升序时间数据
                .assignAscendingTimestamps(_.timestamp * 1000L)

        //2，定义刷单行为过滤操作：将黑名单数据过滤到侧输出流
        val filterStream = adLogStream.keyBy(data => (data.userId, data.adId))
                .process(new FilterBlackList(100))



        //3，按照省份分组开窗聚合
        val resultStream = filterStream.keyBy(_.province)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdCountAgg(), new AdCountResultFunction())

        resultStream.print()

        //4，对于黑名单中的数据，从侧输出流中获取再做其他处理即可
        filterStream.getSideOutput(new OutputTag[BlackListWarning]("blackList")).print("blackList")

        env.execute("ad analysis job")
    }
}

// 定义输入输出样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
case class AdCountByProvince( province: String, windowEnd: String, count: Long)
// 定义侧输出流报警信息样例类
case class BlackListWarning( userId: Long, adId: Long, msg: String)

/**
 * 自定义预聚合函数
 */
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

/**
 * 自定义全窗口函数
 */
class AdCountResultFunction() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
        out.collect(AdCountByProvince(
            key,
            new Timestamp(window.getEnd).toString,
            input.head
        ))
    }
}

/**
 * 自定义过滤器：通过ProcessFunction实现筛查功能
 * @param maxClickCount 设置刷单点击量阈值
 */
class FilterBlackList(maxClickCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {

    //定义状态，需要保存当前用户对当前广告的点击累积量count
    var countState: ValueState[Long] = _
    //标识位，用来表示用户是否已在黑名单中
    var isSentState: ValueState[Boolean] = _

    override def open(parameters: Configuration): Unit = {
        countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
        isSentState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent", classOf[Boolean]))
    }

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
        //首先取出状态数据
        val currentCount = countState.value()

        //如果是第一个数据，那么注册明晨0点的定时器，记录今天该用户对该广告的访问量：用于清空状态
        if (currentCount == 0) {
            //定时器可以使用系统处理时间，因为使用事件时间的话可能定时器会未在恰当时间触发清除当天状态
            val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
            ctx.timerService().registerProcessingTimeTimer(ts)
        }

        //判断countState值是否到达阈值100，如果到达，并且之前没有输出过报警信息，那么报警
        if (currentCount >= maxClickCount) {
            if (!isSentState.value()) {
                ctx.output(new OutputTag[BlackListWarning]("blackList"), BlackListWarning(
                    value.userId,
                    value.adId,
                    "click over " + currentCount + " times today"
                ))
                isSentState.update(true)
            }
            return
        }

        //countState值加一
        countState.update(currentCount + 1)

        //正常的数据则原封不动输出
        out.collect(value)
    }

    //0点触发定时器，直接清空当天所有状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
        countState.clear()
        isSentState.clear()
    }
}


