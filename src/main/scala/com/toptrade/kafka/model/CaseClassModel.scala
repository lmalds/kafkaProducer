package com.toptrade.kafka.model

object CaseClassModel {

  case class TX(date : Int, time : Long, exchange : String, code : String, index : Int, price : Double, volume : Long,
                value : Double, seq : Long, kafkaSysTime : Long)

  case class MD(date : Int, time : Long, exchange : String, code : String, status : Int, price : Double, preClose : Double,
                open : Double, high : Double, low : Double, volume : Long, value : Double, bidPrice1 : Double, askPrice1 : Double,
                kafkaSysTime : Long)

  case class Index(date : Int, time : Long, exchange : String, code : String, price : Double, open : Double, high : Double,
                   low : Double, volume : Long, value : Double, preClose : Double, kafkaSysTime : Long)



}
