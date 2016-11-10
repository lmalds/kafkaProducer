package com.toptrade.kafka.producer

import PB.MSGCARRIER.Msgcarrier.MsgCarrier
import com.toptrade.kafka.model.CaseClassModel.MD
import com.toptrade.zmq.ZmqSub
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import scala.collection.JavaConverters._

class SnapshotProducer extends Runnable{

  val subscriber = ZmqSub.subZmqMD()
  val producer  = Conf.produceMD()
  val topic = "md"

  override def run(): Unit = {
    while(true){
      subscriber.recv()
      val data = subscriber.recv()
      val mc = MsgCarrier.parseFrom(data)
      if(mc.getType == MsgCarrier.MsgType.SNAPSHOT){
        val snapshot = PB.Quote.Quote.SnapShot.parseFrom(mc.getMessage)

        val k = snapshot.getCode
        val kafkaSysTime = System.currentTimeMillis()
        val eventTime = Conf.getLongTime(snapshot.getDate,snapshot.getTime)
        val bidPrice1 = snapshot.getBidpricesList.asScala.head
        val askPrice1 = snapshot.getAskpricesList.asScala.head

        val mdClass = MD(snapshot.getDate,eventTime,snapshot.getExchange.toString,snapshot.getCode,snapshot.getStatus,
          snapshot.getLastprice,snapshot.getPrevclose,snapshot.getOpen,snapshot.getHigh,snapshot.getLow,snapshot.getVolume,
          snapshot.getValue,bidPrice1,askPrice1,kafkaSysTime)

        val v = mdClass

        val data = new ProducerRecord[String, MD](topic,k,v)
        val callBackNew = new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {}
        }

        producer.send(data,callBackNew)
      }
    }
  }
}
