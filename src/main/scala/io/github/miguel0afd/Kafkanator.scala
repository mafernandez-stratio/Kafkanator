package io.github.miguel0afd

import java.util.HashMap

import com.typesafe.scalalogging.Logger
import io.codearte.jfairy.Fairy
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.util.{Failure, Success, Try}

object Defaults {
  val DefaultTopic = "test"
  val DefaultServers = "127.0.0.1:9092"
  val DefaultInterval = (100, 2000)
  val DefaultGroupId = "test"
  val DefaultKeySerializer = "org.apache.kafka.common.serialization.StringSerializer"
  val DefaultValueSerializer = "org.apache.kafka.common.serialization.StringSerializer"
}

object Kafkanator extends App {

  import Defaults._

  val logger = Logger(this.getClass)

  Try(args(0)) match {
    case Success(value) if value.equals("--help") =>
      logger.info(s"Usage: Kafkanator [topic] [bootstrap-servers] [min-sleep] [max-sleep] [group-id]")
      logger.info(s"Default: topic -> test, bootstrap-servers -> localhost:9092, min-sleep -> 100, max-sleep -> 2000, group-id -> test")
      sys.exit()
    case Failure(_) => //No-op
  }

  val topic = Try(args(0)).getOrElse(DefaultTopic)
  val servers = Try(args(1)).getOrElse(DefaultServers)
  val minWait = Try(args(2).toInt).getOrElse(DefaultInterval._1)
  val maxWait = Try(args(3).toInt).getOrElse(DefaultInterval._2)
  val groupId = Try(args(4)).getOrElse(DefaultGroupId)

  assert(maxWait > minWait, "Wrong arguments: max random time must be greater that min random time")

  logger.info(s"TOPIC: $topic")

  val props = new HashMap[String, Object]()
  props.put("bootstrap.servers", servers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("group.id", groupId)
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

  val fairy = Fairy.create()
  val fairyProducer = fairy.baseProducer()

  while(true){

    Thread.sleep(fairy.baseProducer.randomBetween(minWait, maxWait))

    val m = Map(
      "clientId" -> fairyProducer.randomBetween(1, 20),
      "center" -> cities(fairyProducer.randomBetween(0, cities.length-1)),
      "product" -> products(fairyProducer.randomBetween(0, products.length-1)),
      "price" -> fairyProducer.randomBetween(1, 2000)
    )

    val message = new ProducerRecord[String, String](
      topic,
      s"""{"clientId": ${m.get("clientId").get},
         | "center": "${m.get("center").get}",
         | "product": "${m.get("product").get}",
         | "price": ${m.get("price").get}}""".stripMargin.replace(System.lineSeparator(), ""))

    logger.info(s"Record: $message")

    producer.send(
      message,
      new Callback() {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          Option(exception).map{ e =>
            e.printStackTrace()
          }.getOrElse{
            logger.info(s"The offset of the record we just sent is: ${metadata.offset()}")
          }
        }
      }
    )
  }

}

