package io.github.miguel0afd

import java.util.Properties

import io.github.miguel0afd.Kafkanator._
import kafka.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.util.Try

/**
  * Created by miguelangelfernandezdiaz on 07/03/16.
  */
object Readator extends App {

  val topic = Try(args(0)).getOrElse("testTopic")

  println(s"TOPIC: $topic")

  val props = new Properties()
  props.put("zookeeper.connect", "localhost:2181")
  props.put("group.id", "crossdatatest")
  props.put("zookeeper.session.timeout.ms", "500")
  props.put("zookeeper.sync.time.ms", "250")
  props.put("auto.commit.interval.ms", "1000")
  val consumerConfig = new ConsumerConfig(props)

  val topicMap = Map(topic -> 1)

  val consumer = kafka.consumer.Consumer.create(consumerConfig)

  val consumerStreamsMap = consumer.createMessageStreams(topicMap)

  val streamList = consumerStreamsMap.get(topic)

  val deserializer = new StringDeserializer()

  for (stream <- streamList) {
    for (event <- stream){
      for(row <- event){
        val result = deserializer.deserialize(topic, row.message)
        println(s"RESULT: $result")
      }
    }

  }
}
