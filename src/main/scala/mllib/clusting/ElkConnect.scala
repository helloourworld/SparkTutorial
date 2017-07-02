package cluster

// import com.huawei.hadoop.security.LoginUtil
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.{Connection, DriverManager, ResultSet}
import org.postgresql.Driver


object ElkConnect {
  def main(args: Array[String]) {


    val userPrincipal = "admin"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/krb5.conf"
    val hadoopConf: Configuration  = new Configuration()
    // LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);
    val sparkConf = new SparkConf()
    sparkConf.setAppName("ElkConnect").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    // Configure the CheckPoint directory for the Streaming.
    // This parameter is mandatory because of existence of the window concept.

    // Load the driver
    // classOf[org.postgresql.Driver]

    // Setup the connection
    val sourceURL = "jdbc:postgresql://171.0.11.3:25108/poc"
    val username = "omm"
    val password = "Gaussdba@Mpp"
    val conn = DriverManager.getConnection(sourceURL, username, password)
//    val conn_str = "jdbc:postgresql://192.9.145.206:25108/postgres?user=omm&password=Gaussdba@Mpp"
//    val conn = DriverManager.getConnection(conn_str)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      //      KMeansELK
      val result = statement.executeQuery("select * from his_realtime limit 50;")
      val resultMetaData = result.getMetaData
      val colNum = resultMetaData.getColumnCount()
      for (i <- 1 to colNum) {
        print(resultMetaData.getColumnLabel(i) + "\t")
      }
      // Iterate Over ResultSet
      println()

      while (result.next) {
        for (i <- 1 to colNum) {
          print(result.getString(i) + "\t")
        }
        println()
      }
    }
    finally {
      conn.close
    }
  }
}

