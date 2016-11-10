package com.toptrade.kafka.producer

import PB.MSGCARRIER.Msgcarrier.MsgCarrier
import com.toptrade.kafka.model.CaseClassModel.Index
import com.toptrade.zmq.ZmqSub
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}


class IndexProducer extends Runnable{

  val subscriber = ZmqSub.subZmqIndex()
  val producer  = Conf.produceIndex()
  val topic = "ind"

  override def run(): Unit = {
    while(true){
      subscriber.recv()
      val data = subscriber.recv()
      val mc = MsgCarrier.parseFrom(data)
      if(mc.getType == MsgCarrier.MsgType.INDEX){
        val index = PB.Quote.Quote.Index.parseFrom(mc.getMessage)

        val k = index.getCode
        val kafkaSysTime = System.currentTimeMillis()
        val eventTime = Conf.getLongTime(index.getDate,index.getTime)

        val indexClass = Index(index.getDate,eventTime,index.getExchange.toString,index.getCode,index.getLastIndex,index.getOpenIndex,
          index.getHighIndex,index.getLowIndex,index.getTotalVolume,index.getTurnover,index.getPreCloseIndex,kafkaSysTime)

        val v = indexClass

        val data = new ProducerRecord[String, Index](topic,k,v)
        val callBackNew = new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {}
        }

        producer.send(data,callBackNew)
      }
    }
  }
}
