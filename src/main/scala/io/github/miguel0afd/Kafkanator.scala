package io.github.miguel0afd

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Try

object Kafkanator extends App {

  val topic = Try(args(0)).getOrElse("inputTopic")

  val host = Try(args(1)).getOrElse("localhost")

  println(s"TOPIC: $topic")

  val props = new HashMap[String, Object]()
  props.put("bootstrap.servers", s"$host:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("group.id", "xd1")
  val producer = new KafkaProducer[String, String](props)

  val products = List(
    "Shoes",
    "Shirt",
    "Apple",
    "Laptop",
    "Television",
    "Telephone",
    "Orange",
    "Computer",
    "Bread",
    "Chair",
    "Table",
    "Chicken",
    "Pencil")

  val cities = List(
    "Chicago",
    "Houston",
    "Philadelphia",
    "Phoenix",
    "Portland",
    "Boston",
    "Seattle"
  )

  while(true){

    Thread.sleep(Utils.randomInt(100, 2000))

    val m = Map(
      "clientId" -> Utils.randomInt(1, 20),
      "center" -> cities(Utils.randomInt(0, cities.length-1)),
      "product" -> products(Utils.randomInt(0, products.length-1)),
      "price" -> Utils.randomInt(1, 2000)
    )

    val message = new ProducerRecord[String, String](
      topic,
      s"""{"clientId": ${m.get("clientId").get},
         | "center": "${m.get("center").get}",
         | "product": "${m.get("product").get}",
         | "price": ${m.get("price").get}}""".stripMargin.replace(System.lineSeparator(), ""))

    println(s"Record: $message")

    producer.send(message)
  }

}

