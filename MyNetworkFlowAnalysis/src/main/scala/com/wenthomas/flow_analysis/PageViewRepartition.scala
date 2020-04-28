package com.wenthomas.flow_analysis
import com.wenthomas.flow_analysis.PageView.getClass
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-27 14:58 
 */
object PageViewRepartition {
    def main(args: Array[String]): Unit = {
        //0，创建一个流处理执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(4)
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //1，从文件中读取数据
        //todo:读不到文件
        val inputStream = env.readTextFile("E:\\project\\MyUserBehaviorAnalysis\\MyNetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
        val dataStream = inputStream.map(data => {
            val dataArray = data.split(",")
            UserBehaviorPV(
                dataArray(0).toLong,
                dataArray(1).toLong,
                dataArray(2).toInt,
                dataArray(3),
                dataArray(4).toLong)
        })
                .assignAscendingTimestamps(_.timestamp * 1000)

        //2，以数据所在分区为key（目的使数据分散，不聚拢在一个分区造成数据倾斜），算出各分区的PvCount
        val aggStream = dataStream.filter(_.behavior == "pv")
                //自定义Map，将key均匀分配，减少数据倾斜
                .map(new MyMapper())
                .keyBy(_._1)
                .timeWindow(Time.seconds(60 * 60))
                .aggregate(new PvCountAgg(), new PvCountResult())

        aggStream.print("aggStream")

        //3，把个分区的结果汇总在一起，各分区以windowEnd分组，把各时间窗口的数据最终分别汇总输出
        val resultStream = aggStream.keyBy(_.windowEnd)
                .process(new TotalPvCountResult())

        resultStream.print("resultStream")

        env.execute("PV Job")
    }

}

case class UserBehaviorPV(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class PvCount( windowEnd: Long, count: Long )

class MyMapper() extends RichMapFunction[UserBehaviorPV, (String, Long)] {

    var index: Long = _

    override def open(parameters: Configuration): Unit = {
        //获取数据所在分区
        index = getRuntimeContext.getIndexOfThisSubtask
    }

    override def map(value: UserBehaviorPV): (String, Long) = {
        //直接以数据所在分区作为key，使数据分步更均匀一些
        (index.toString, 1L)
    }
}

class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class PvCountResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
        out.collect(PvCount(window.getEnd, input.head))
    }
}

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {

    //定义一个状态用来保存当前所有结果之和
    var totalCountState: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
        totalCountState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))
    }

    override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
        //加上新的count值，更新状态
        totalCountState.update(totalCountState.value() + value.count)
        //注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
        //定时器触发时，所有分区count值都已到达，输出总和
        out.collect(PvCount(ctx.getCurrentKey, totalCountState.value()))
        totalCountState.clear()
    }
}