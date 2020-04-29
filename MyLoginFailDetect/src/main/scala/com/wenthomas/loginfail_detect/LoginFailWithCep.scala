package com.wenthomas.loginfail_detect
import java.util

import com.wenthomas.loginfail_detect.LoginFail.getClass
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @author Verno
 * @create 2020-04-29 3:47 
 */
object LoginFailWithCep {
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


        //---------------------------------------Flink CEP--------------------------------------------------------------
        //1，定义匹配的模式
        //注意：使用.times循环模式默认的模式序列为非严格近邻，需要额外的配置才能设定为严格近邻。故这里没有采用.times
        val loginFailPattern = Pattern.begin[LoginEvent]("firstFail").where(_.eventType == "fail")
                .next("secondFail").where(_.eventType == "fail")
                .within(Time.seconds(2))

        //2，在分组之后的数据流上应用模式，得到一个PatternStream
        //input为loginEventStream.keyBy(_.userId)，以各个user分组进行匹配
        val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

        //3，将检测到的事件序列，转换输出报警信息
        val loginFailStream = patternStream.select(new LoginFailDetect())

        loginFailStream.print()

        env.execute("Login Fail With cep Job")
    }

}

/**
 * 自定义PatternSelectFunction：用来将检测到的连续登录失败事件，包装成报警信息输出
 */
class LoginFailDetect() extends PatternSelectFunction[LoginEvent, Warning] {
    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
        //map里存放的就是匹配到的一组事件， key是定义好的事件模式名称
        val firstLoginFail = map.get("firstFail").get(0)
        val lastLoginFail = map.get("secondFail").get(0)
        Warning(
            firstLoginFail.userId,
            firstLoginFail.eventTime,
            lastLoginFail.eventTime,
            "Login Fail!"
        )
    }
}
