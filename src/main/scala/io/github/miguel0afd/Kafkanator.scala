package io.github.miguel0afd

import java.util.HashMap

import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by miguelangelfernandezdiaz on 04/03/16.
  */
class Kafkanator {

  val props = new HashMap[String, Object]()
  val producer = new KafkaProducer[String, String](props)

}
