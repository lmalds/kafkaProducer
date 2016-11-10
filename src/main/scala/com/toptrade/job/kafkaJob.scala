package com.toptrade.job

import com.toptrade.kafka.producer.{IndexProducer, SnapshotProducer, TransactionProducer}

object kafkaJob {

  def main(args : Array[String]) : Unit = {
    new Thread(new TransactionProducer).start()
    //new Thread(new SnapshotProducer).start()
    //new Thread(new IndexProducer).start()
  }

}
