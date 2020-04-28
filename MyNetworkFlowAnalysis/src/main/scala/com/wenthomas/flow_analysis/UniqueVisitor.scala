package com.wenthomas.flow_analysis
import com.wenthomas.flow_analysis.PageView.getClass
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-27 5:35 
 */
object UniqueVisitor {
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

        //2，使用全窗口函数一次处理窗口内所有数据，存入set中去重userId求出count
        val resultStream = dataStream.filter(_.behavior == "pv")
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountByWindow())

        resultStream.print()

        env.execute("UV Count Job")
    }
}

case class UvCount(windowEnd: Long, count: Long)

/**
 * 自定义全窗口函数
 */
class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        //定义一个scala set，用于保存所有的数据userId并去重
        var idSet = Set[Long]()
        //把当前窗口所有数据的ID收集到set中，最后输出set的大小
        for (userBehavior <- input) {
            idSet += userBehavior.userId
        }
        out.collect(UvCount(window.getEnd, idSet.size))
    }
}
