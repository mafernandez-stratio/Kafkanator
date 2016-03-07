package io.github.miguel0afd

import java.util.Properties

import kafka.consumer.ConsumerConfig
import kafka.message.{Message, MessageAndMetadata}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Created by miguelangelfernandezdiaz on 07/03/16.
  */
object Readator extends App {

  val props = new Properties()
  props.put("zookeeper.connect", "localhost:2181")
  props.put("group.id", "crossdatatest")
  props.put("zookeeper.session.timeout.ms", "500")
  props.put("zookeeper.sync.time.ms", "250")
  props.put("auto.commit.interval.ms", "1000")
  val consumerConfig = new ConsumerConfig(props)

  val topicMap = Map("testTopic" -> 1)

  val consumer = kafka.consumer.Consumer.create(consumerConfig)

  val consumerStreamsMap = consumer.createMessageStreams(topicMap)

  val streamList = consumerStreamsMap.get("testTopic")

  val deserializer = new StringDeserializer()

  for (stream <- streamList) {
    for (event <- stream){
      for(row <- event){
        val result = deserializer.deserialize("testTopic", row.message)
        println(s"RESULT: $result")
      }
    }

  }
}
