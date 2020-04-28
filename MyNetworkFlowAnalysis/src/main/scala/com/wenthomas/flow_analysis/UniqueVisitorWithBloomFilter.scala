package com.wenthomas.flow_analysis
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
/**
 * @author Verno
 * @create 2020-04-28 1:47 
 */
/**
 * 使用布隆过滤器，解决数据量大时直接保存状态到set中导致的内存占用高的问题，通过存储在布隆过滤器中的位图来解决优化
 */
object UniqueVisitorWithBloomFilter {
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

        //2，分配key，包装成二元组开窗聚合
        val resultStream = dataStream.filter(_.behavior == "pv")
                .map(data => ("uv", data.userId))
                //伪分组：使数据可以使用全窗口函数
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                //Trigger: 定义 window 什么时候关闭，触发计算并输出结果
                //设定窗口触发器策略：每个元素来到都触发计算并清空状态
                .trigger(new MyTrigger())
                //全窗口函数
                .process(new UvCountResultWithBloomFilter())

        resultStream.print()

        env.execute("UV Count With BloomFilter Job")
    }

}

/**
 * 自定义窗口触发器：定义 window 什么时候关闭，触发计算并输出结果
 * 需求：每来一条数据就触发一次窗口计算操作
 */
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    //定义策略：数据来了以后，触发计算并清空状态，不保存数据
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    //clear不做任何处理
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

/**
 * 自定义布隆过滤器
 * @param size
 */
class Bloom(size: Long) extends Serializable {
    //定义位图的大小，应为2的整次幂
    private val cap = size

    //实现一个hash函数：应尽量使数据稀疏分步，不发生哈希碰撞，使每个数据占有一个独有空间（位图的一个单位）
    def hash(str: String, seed: Int) = {
        var result = 0
        for (i <- 0 until str.length) {
            result = result * seed + str.charAt(i)
        }
        //返回一个在cap范围内的值
        (cap - 1) & result
    }
}

class UvCountResultWithBloomFilter() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

    var jedis: Jedis = _
    var bloom: Bloom = _

    override def open(parameters: Configuration): Unit = {
        jedis = new Jedis("localhost", 6379)
        //位图大小10亿位，即2^30，占用128MB
        bloom = new Bloom(1 << 30)
    }

    //每当来一个数据，通过布隆过滤器判断Redis位图中对应位置是否为1
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
        //bitmap用当前窗口的end作为key，保存在redis中：（windowEnd, bitmap）
        val storeKey = context.window.getEnd.toString

        //我们把每个窗口的uv count的值，作为状态写入Redis中，存为一张countMap的表
        val countMap = "countMap"
        //先获取当前的count值
        var count = 0L
        if (jedis.hget(countMap, storeKey) != null) {
            count = jedis.hget(countMap, storeKey).toLong
        }

        //取userId, 计算hash值， 判断是否在bitmap中
        val userId = elements.last._2.toString
        //布隆过滤器的hash种子尽量设一个大数，使算出hash值更加离散
        val offset = bloom.hash(userId, 61)
        val isExist = jedis.getbit(storeKey, offset)

        //如果不存在，则将对应位置置1，count + 1；反之不做操作
        if (!isExist) {
            jedis.setbit(storeKey, offset, true)
            jedis.hset(countMap, storeKey, (count + 1).toString)
        }
    }
}
