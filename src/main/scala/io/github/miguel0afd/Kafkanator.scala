package io.github.miguel0afd

import java.util.HashMap

import com.typesafe.scalalogging.Logger
import io.codearte.jfairy.Fairy
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.util.{Failure, Success, Try}

object Defaults {
  val DefaultTopic = "kafkanator"
  val DefaultServers = "127.0.0.1:9092"
  val DefaultInterval = (100, 2000)
  val DefaultGroupId = "kafkanator"
  val DefaultKeySerializer = "org.apache.kafka.common.serialization.StringSerializer"
  val DefaultValueSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  val KeyClient = "clientId"
  val KeyCenter = "center"
  val KeyProduct = "product"
  val KeyPrice = "price"

}

object Kafkanator extends App {

  import Defaults._

  val logger = Logger(this.getClass)

  Try(args(0)) match {
    case Success(value) if value.equals("--help") =>
      logger.info(s"Usage: Kafkanator [topic] [bootstrap-servers] [min-sleep] [max-sleep] [group-id]")
      logger.info(s"Default: topic -> kafkanator, bootstrap-servers -> localhost:9092, min-sleep -> 100, max-sleep -> 2000, group-id -> kafkanator")
      sys.exit()
    case Failure(_) =>
      logger.info("Starting Kafkanator")
  }

  val topic = Try(args(0)).getOrElse(DefaultTopic)
  val servers = Try(args(1)).getOrElse(DefaultServers)
  val minWait = Try(args(2).toInt).getOrElse(DefaultInterval._1)
  val maxWait = Try(args(3).toInt).getOrElse(DefaultInterval._2)
  val groupId = Try(args(4)).getOrElse(DefaultGroupId)

  assert(maxWait > minWait, "Wrong arguments: max random time must be greater that min random time")

  logger.info(s"Topic: $topic")

  val props = new HashMap[String, Object]()
  props.put("bootstrap.servers", servers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("group.id", groupId)
  props.put("delivery.timeout.ms", 30000)
  props.put("acks", "all")
  props.put("linger.ms", 1)
  props.put("acks", 1)
  props.put("buffer.memory", 33554432)
  props.put("retries", 10)
  props.put("batch.size", 16384)
  props.put("connections.max.idle.ms", 540000)
  val producer = new KafkaProducer[String, String](props)

  val products = List(
    "Shoes",
    "Shirt",
    "Apple",
    "Laptop",
    "Television",
    "Telephone",
    "Orange",
    "Watermelon",
    "Strawberries",
    "Computer",
    "Bread",
    "Chair",
    "Table",
    "Chicken",
    "Pencil",
    "Meat",
    "Trousers",
    "Camera"
  )

  val cities = List(
    "Chicago",
    "Houston",
    "Philadelphia",
    "Phoenix",
    "Portland",
    "Boston",
    "Seattle",
    "Miami",
    "Detroit",
    "Milwaukee",
    "Austin",
    "Charleston"
  )

  val fairy = Fairy.create()
  val fairyProducer = fairy.baseProducer()

  while(true){

    Thread.sleep(fairy.baseProducer.randomBetween(minWait, maxWait))

    val m = Map(
      KeyClient -> fairyProducer.randomBetween(1, 100),
      KeyCenter -> cities(fairyProducer.randomBetween(0, cities.length-1)),
      KeyProduct -> products(fairyProducer.randomBetween(0, products.length-1)),
      KeyPrice -> fairyProducer.randomBetween(1, 2000)
    )

    val message = new ProducerRecord[String, String](
      topic,
      s"""{"$KeyClient": ${m.getOrElse(KeyClient, null)},
         | "$KeyCenter": "${m.getOrElse(KeyCenter, null)}",
         | "$KeyProduct": "${m.getOrElse(KeyProduct, null)}",
         | "$KeyProduct": ${m.getOrElse(KeyPrice, null)}}""".stripMargin.replace(System.lineSeparator, ""))

    logger.info(s"Record: $message")

    producer.send(
      message,
      new Callback() {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          Option(exception).map{ e =>
            logger.error(s"Message '$message' couldn't be sent", e)
            e.printStackTrace()
          }.getOrElse{
            logger.info(s"The offset of the record sent is: ${metadata.offset}")
          }
        }
      }
    )
  }

}

