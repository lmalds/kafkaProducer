package com.toptrade.kafka.producer

import java.text.SimpleDateFormat
import java.util.Properties

import com.toptrade.kafka.model.CaseClassModel.{Index, MD, TX}
import org.apache.kafka.clients.producer.KafkaProducer

object Conf {

  private val BROKER_LIST = "flink:9092,data0:9092,mf:9092"

  private val props = new Properties()
  props.put("bootstrap.servers", this.BROKER_LIST)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "com.toptrade.kafka.serializer.PojoSerializer")
  props.put("producer.type", "async")

  def produceTX() : KafkaProducer[String,TX] = {
    new KafkaProducer[String, TX](this.props)
  }

  def produceMD() : KafkaProducer[String,MD] = {
    new KafkaProducer[String, MD](this.props)
  }

  def produceIndex() : KafkaProducer[String,Index] = {
    new KafkaProducer[String, Index](this.props)
  }

  def getLongTime(date : Int, time : Int) : Long = {
    val format = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")
    val dateInString = date.toString + " 00:00:00.000"
    val dateInDate = format.parse(dateInString)
    val dateInLong = dateInDate.getTime

    val timeInLong = dateInLong + time
    timeInLong
  }

}
