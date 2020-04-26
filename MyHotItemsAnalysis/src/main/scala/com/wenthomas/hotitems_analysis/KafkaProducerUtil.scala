package com.wenthomas.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @author Verno
 * @create 2020-04-26 23:28 
 */
object KafkaProducerUtil {
    def main(args: Array[String]): Unit = {
        writeToKafkaWithTopic("my_hotitems")
    }

    def writeToKafkaWithTopic(topic: String) = {
        val props = new Properties()
        props.setProperty("bootstrap.servers", "mydata01:9092")
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        //创建生产者
        val producer = new KafkaProducer[String, String](props)

        //从文件中读取数据，逐条发送
        val bufferdSource = io.Source.fromFile("E:\\project\\MyUserBehaviorAnalysis\\MyHotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        for (line <- bufferdSource.getLines()) {
            val record = new ProducerRecord[String, String](topic, line)
            producer.send(record)
        }

        producer.close()
    }
}
