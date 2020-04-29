package com.wenthomas.market_analysis
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-29 0:27 
 */
/**
 * APP市场推广统计：不分渠道（总量）统计
 */
object AppMarketingTotal {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //1，从自定义数据源输入数据
        val inputStream = env.addSource(new SimulateMarketEventSource()).assignAscendingTimestamps(_.timestamp)

        //2，开窗聚合
        val resultStream = inputStream.filter(_.behavior != "UNINSTALL")
                .map(data => ("total", 1L))
                .keyBy(_._1)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketCountAgg(), new MarketCountResult())

        resultStream.print("result total")

        env.execute("market count by total")
    }

}

class MarketCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

//自定义WindowFunction
/*class MarketCountResult() extends WindowFunction[Long, MarketCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketCount]): Unit = {
        out.collect(MarketCount(
            window.getStart.toString,
            window.getEnd.toString,
            "total",
            "total",
            input.head
        ))
    }
}*/

//自定义ProcessWindowFunction
class MarketCountResult() extends ProcessWindowFunction[Long, MarketCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[MarketCount]): Unit = {
        out.collect(MarketCount(
            context.window.getStart.toString,
            context.window.getEnd.toString,
            "total",
            "total",
            elements.head
        ))
    }
}
