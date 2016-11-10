package com.toptrade.kafka.producer

import PB.MSGCARRIER.Msgcarrier.MsgCarrier
import com.toptrade.kafka.model.CaseClassModel.TX
import com.toptrade.zmq.ZmqSub
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

class TransactionProducer extends Runnable{

  /**
    * sub Zmq -- Transaction
    */
  val subscriber = ZmqSub.subZmqTX()

  /**
    * get kafka produce config
    */
  val producer  = Conf.produceTX()
  val topic = "tx"


  override def run(): Unit = {

    while(true){
      subscriber.recv()
      val data = subscriber.recv()
      val mc = MsgCarrier.parseFrom(data)
      if(mc.getType == MsgCarrier.MsgType.TRANSACTIONS){
        val transactions = PB.Quote.Quote.Transactions.parseFrom(mc.getMessage)
        val itemCount = transactions.getItemsCount
        for(i <- 0 until itemCount){
          val transaction = transactions.getItems(i)
          val k = transaction.getCode

          val kafkaSysTime = System.currentTimeMillis()
          val eventTime = Conf.getLongTime(transaction.getDate,transaction.getTime)
          val txClass = TX(transaction.getDate,eventTime,transaction.getExchange.toString,transaction.getCode,transaction.getNIndex,transaction.getLastprice,
            transaction.getVolume,transaction.getTurnover,transaction.getSeqno,kafkaSysTime)

          val v = txClass

          val data = new ProducerRecord[String, TX](topic,k,v)
          val callBackNew = new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {}
          }

          producer.send(data,callBackNew)
        }
      }

    }
  }
}
