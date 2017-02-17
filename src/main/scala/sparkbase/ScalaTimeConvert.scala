package main

import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
  * Created by david on 2016/11/12.
  */
object ScalaTimeConvert {
  def main(args: Array[String]): Unit ={
    println(getTimeByString("1478923800000"))
    println(getTimeByString("20151021235349"))
  }
  def getTimeByString(timeString: String): Long = {
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    sf.parse(timeString).getTime / 1000
  }
}
