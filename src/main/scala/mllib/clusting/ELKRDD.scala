package cluster

import java.sql.{DriverManager, ResultSet}

// import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ELKRDD {
  def main(args: Array[String]) {
    val userPrincipal = "admin"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/krb5.conf"
    val hadoopConf: Configuration = new Configuration()
    // LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("ELKRDD").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    // Load the driver
    //    classOf[org.postgresql.Driver]

    // Setup the connection
    val sourceURL = "jdbc:postgresql://171.0.11.6:25108/poc"
    val username = "omm"
    val password = "Gaussdba@Mpp"
    val url = "jdbc:postgresql://171.0.11.6:25108/poc?user=omm&password=Gaussdba@Mpp"
    DriverManager.setLoginTimeout(180)
    val conn = DriverManager.getConnection(sourceURL, username, password)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      val result = statement.executeQuery("select init_date,id,entrust_type from his_realtime limit 50;")
      val resultMetaData = result.getMetaData
      val colNum = resultMetaData.getColumnCount
      println(colNum)
      for (i <- 1 to colNum) {
        print(resultMetaData.getColumnLabel(i) + "\t")
      }
      println()
      val resultSetList = Iterator.continually((result.next(), result)).takeWhile(_._1).map(r =>
        (r._2.getString(2),
          r._2.getString(3))
      ).toList
      val resRDD = sc.parallelize(resultSetList, 4)
      println(resRDD)
      resRDD.foreach(println)
//      statement.executeUpdate("CREATE TABLE cluster_result(id VARCHAR(32), name VARCHAR(32));");

//      val schema = StructType(
//        StructField("id", StringType)::
//        StructField("name", StringType)
//        ::Nil
//      )
//      val data = resRDD.map(item => Row.apply(item._1, item._2))
//      val df = sqlContext.createDataFrame(data, schema)
//      //df.collect()
//      df.createJDBCTable (url,"sparktomysql", false)
//      resRDD.foreach {line =>
//        val sql = "insert into cluster_result(id, name) values (" + line._1 + ", " + line._2 + ");"
//        println(sql)
//      }
    }

    finally {
      conn.close()
    }
  }
}
