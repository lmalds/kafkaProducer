package com.toptrade.zmq

import org.zeromq.ZMQ
import org.zeromq.ZMQ.Socket


object ZmqSub {

  /**
    *  ZeroMQ sub info
    *  sub 3 sources: transaction & snapshot & index
    */
  private val PUBLISHER_ADDRESS = "tcp://192.168.4.21:8147"

  private val TRANSACTION_TYPE = "94."
  private val SNAPSHOT_TYPE = "26."
  private val INDEX_TYPE = "221."

  private val context = ZMQ.context(1)
  private val subscriber  = context.socket(ZMQ.SUB)
  subscriber.connect(PUBLISHER_ADDRESS)


  def subZmqTX() : Socket = {
    subscriber.subscribe(TRANSACTION_TYPE.getBytes)
    subscriber
  }

  def subZmqMD() : Socket = {
    subscriber.subscribe(SNAPSHOT_TYPE.getBytes)
    subscriber
  }

  def subZmqIndex() : Socket = {
    subscriber.subscribe(INDEX_TYPE.getBytes)
    subscriber
  }


}
