package com.wenthomas.loginfail_detect
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
 * @author Verno
 * @create 2020-04-29 3:25 
 */
/**
 * 恶意登录监控：状态编程实现
 * 需求：2s内同一用户2次以上登录失败即报警
 *
 * 缺点：
 *  1，当2s内多条失败记录到来，需要等到窗口结束后才告警，告警不够实时
 *  2，当2s内多条记录中若有一条登录成功，按照代码实现会算作正常事件，不告警
 *
 * 优化：
 *  1，使用Flink CEP实现
 *  2，对每个到来的数据需要注册多一个定时器，实现逻辑太复杂，不推荐
 */
object LoginFail {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //0，从文件中读取数据，map为样例类，并分配时间戳和WM
        val resource = getClass.getResource("/LoginLog.csv")
        val inputStream = env.readTextFile(resource.getPath)
        val loginEventStream = inputStream.map(data => {
            val dataArray = data.split(",")
            LoginEvent(
                dataArray(0).toLong,
                dataArray(1),
                dataArray(2),
                dataArray(3).toLong
            )
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
                    override def extractTimestamp(element: LoginEvent): Long = {
                        element.eventTime * 1000L
                    }
                })

        //1，用ProcessFunction进行转换，如果遇到2s内连续两次登录失败，就输出报警
        val resultStream = loginEventStream.keyBy(_.userId)
                .process(new LoginFailWarningFunction(2))

        resultStream.print()

        env.execute("login fail job")
    }
}

// 定义输入输出的样例类
case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String )

/**
 * 自定义ProcessFunction函数：实现检测2s内多次登录失败事件
 * @param maxFailTimes: 单位窗口内最大失败登录次数
 */
class LoginFailWarningFunction(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

    //定义List状态，用来保存2s内所有的登录失败事件
    var loginFailListState: ListState[LoginEvent] = _

    //定义value状态，用来保存定时器的时间戳，方便对需要的定时器进行操作
    var timerTsState: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
        loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved-loginfail", classOf[LoginEvent]))
        timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
    }

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
        //首先判断当前数据是否登录失败
        if (value.eventType == "fail") {
            //如果是失败，那么添加到ListState中，如果没有注册过定时器，则注册
            loginFailListState.add(value)
            if (timerTsState.value() == 0) {
                //设置2s后的定时器
                val ts = value.eventTime * 1000L + 2000L
                ctx.timerService().registerEventTimeTimer(ts)
                timerTsState.update(ts)
            }
        } else {
            //如果登录成功，则删除当前定时器，重新开始
            ctx.timerService().deleteEventTimeTimer(timerTsState.value())
            loginFailListState.clear()
            timerTsState.clear()
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
        //如果2s后的定时器触发了，那么判断ListState中失败的个数
        val allLoginFailList = new ListBuffer[LoginEvent]
        val iter = loginFailListState.get().iterator()
        while (iter.hasNext) {
            allLoginFailList += iter.next()
        }

        if (allLoginFailList.length >= maxFailTimes) {
            out.collect(Warning(
                ctx.getCurrentKey,
                allLoginFailList.head.eventTime,
                allLoginFailList.last.eventTime,
                "Login fail in 2s for " + allLoginFailList.length + " times."
            ))
        }

        //清空状态
        loginFailListState.clear()
        timerTsState.clear()
    }
}