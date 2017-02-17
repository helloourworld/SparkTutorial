package main.scala.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


/**
  * Create table in hbase.
  */
object TableCreation {
  def main(args: Array[String]): Unit = {
    val userPrincipal = "sparkuser";
    val userKeytabPath = "/opt/FIclient/user.keytab";
    val krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";
    val ZKServerPrincipal = "zookeeper/hadoop.hadoop.com"

    val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME: String = "Client"
    val ZOOKEEPER_SERVER_PRINCIPAL_KEY: String = "zookeeper.server.principal"
    val hadoopConf: Configuration = new Configuration();

    // Create the configuration parameter to connect the HBase. The hbase-site.xml must be included in the classpath.
    val conf: SparkConf = new SparkConf
    val sc: SparkContext = new SparkContext(conf)
    val hbConf: Configuration = HBaseConfiguration.create(sc.hadoopConfiguration)

    // Create the connection channel to connect the HBase
    val connection: Connection = ConnectionFactory.createConnection(hbConf)

    // Declare the description of the table
    val userTable = TableName.valueOf("shb1")
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

    // Create a table
    println("Creating table shb1. ")
    val admin = connection.getAdmin
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)

    connection.close()
    sc.stop()
    println("Done!")
  }
}
