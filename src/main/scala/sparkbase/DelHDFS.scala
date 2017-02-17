package main.scala.sparkbase

/**
  * Created by hadoop on 2016/11/16.
  */
object DelHDFS {
  def main(args: Array[String]): Unit = {
    delete("hdfs://NN01.HadoopVM:9000","/user/hadoop/target/tmp/myCollaborativeFilter/metadata")
  }

  def delete(master:String,path:String): Unit ={
    println("Begin delete!--" + master+path)
    val output = new org.apache.hadoop.fs.Path(master+path)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(master), new org.apache.hadoop.conf.Configuration())
    // 删除输出目录
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + master+path)
    } else {
      println(master+path + " doesn't exist!")
    }

  }
}
