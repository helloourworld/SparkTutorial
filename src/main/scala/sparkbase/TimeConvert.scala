package main
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
/**
  * Created by david on 2016/11/12.
  */
object TimeConvert {
  def main(args: Array[String]): Unit ={
    println(getTimestamp("20151021235349"))
  }
  def getTimestamp(x:String) :java.sql.Timestamp = {
    //       "20151021235349"
    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    var ts = new Timestamp(System.currentTimeMillis());
    try {
      if (x == "")
        return null
      else {
        val d = format.parse(x);
        val t = new Timestamp(d.getTime());
        return t
      }
    } catch {
      case e: Exception => println("cdr parse timestamp wrong")
    }
    return null
  }
}
