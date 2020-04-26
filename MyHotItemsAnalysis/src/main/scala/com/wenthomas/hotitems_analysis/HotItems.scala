package com.wenthomas.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
 * @author Verno
 * @create 2020-04-25 15:41 
 */
/**
 * 功能需求：每隔5分钟输出最近一小时内点击量最多的前N个商品。
 */
object HotItems {
    def main(args: Array[String]): Unit = {
        //0，创建一个流处理执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //1，从文件中读取数据
//        val inputStream = env.readTextFile("E:\\project\\MyUserBehaviorAnalysis\\MyHotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

        //1，从kafka中读取数据
        val props = new Properties()
        props.setProperty("bootstrap.servers", "mydata01:9092")
        props.setProperty("group.id", "consumer-group")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("auto.offset.reset", "latest")
        val inputStream = env.addSource(new FlinkKafkaConsumer[String]("my_hotitems", new SimpleStringSchema(), props))
        val dataStream = inputStream.map(data => {
            val dataArray = data.split(",")
            UserBehavior(
                dataArray(0).toLong,
                dataArray(1).toLong,
                dataArray(2).toInt,
                dataArray(3),
                dataArray(4).toLong)
        })
                //原数据已排好序，直接配置使用升序时间
                .assignAscendingTimestamps(_.timestamp * 1000L)

        //2，对数据进行转换，过滤出pv行为，开窗聚合统计个数
        //输出的是不同时间窗口下的item和count元组
        val aggStream = dataStream.filter(_.behavior == "pv")
                //按照itemId分组
                .keyBy("itemId")
                //开启滑动窗口
                .timeWindow(Time.minutes(60), Time.minutes(15))
                //定义滑动窗口
                .aggregate(new CountAgg(), new ItemCountWindowResult())

        //todo:对窗口聚合结果按照窗口尽心个分组，并做排序取出TopN输出
        val resultStream = aggStream.keyBy("windowEnd")
                .process(new TopNHotItem(5))

        resultStream.print()

        env.execute("hot items job")
    }
}

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/**
 * 窗口函数聚合输出样例类
 * @param itemId
 * @param windowEnd
 * @param count
 */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
 * 自定义预聚合函数，来一条数据就+1
 */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

/**
 * 自定义窗口函数，结合window信息包装成样例类
 */
class ItemCountWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val itemId = key.asInstanceOf[Tuple1[Long]].f0
        val count = input.iterator.next()
        out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
}

/**
 * 自定义KeyedProcessFunction：用以实现各窗口内排序
 * @param n
 */
class TopNHotItem(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

    private var itemCountListState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
        itemCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemcount-list", classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
        //每来一条数据，就把它保存在状态中
        itemCountListState.add(value)
        //注册定时器，在windowEnd + 100ms时触发
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    //定时器触发，从状态中取出数据，然后排序输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        //先把状态中的数据提取到ListBuffer中
        val allItemCountList: ListBuffer[ItemViewCount] = ListBuffer()
        import scala.collection.JavaConversions._
        for (itemCount <- itemCountListState.get()) {
            allItemCountList += itemCount
        }

        //按照count值的大小倒序排序，取出TopN
        val sortedItemCountList = allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

        //清除状态：统计过的数据清除掉，这样状态中永远只有下一个窗口将要用来统计的状态数据
        itemCountListState.clear()

        //将排名信息输出显示
        val result = new StringBuilder
        result.append("时间： ").append(new Timestamp(timestamp - 100)).append("\n")
        //遍历sorted列表取出TopN信息
        for (i <- sortedItemCountList.indices) {
            val currentItemCount = sortedItemCountList(i)
            result.append("Top").append(i + 1).append(":")
                    .append(" 商品ID=").append(currentItemCount.itemId)
                    .append(" 访问量=").append(currentItemCount.count)
                    .append("\n")
        }
        result.append("----------------------------------------------\n\n")

        Thread.sleep(1000)
        out.collect(result.toString())
    }
}
