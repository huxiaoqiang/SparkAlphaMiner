package cn.edu.tsinghua.thssbpm

import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scopt.OptionParser
case class Config(filePath:String=null,appName:String="alpha miner")
object Main {

  var parser = new scopt.OptionParser[Config]("scopt") {
    head("spark-alpha","0.0.1")

    opt[String]('f',"filePath")
      .required()
      .action((x,c)=>c.copy(filePath=x))
      .text("filePath is the event log file path")

    opt[String]("appName")
      .action((x,c)=>c.copy(appName = x))
      .text("appName is the application name, default is 'alpha miner'")
  }

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        // do stuff
        val filePath = config.filePath
        val appName = config.appName

        //Set spark conf
        val conf = new SparkConf().setAppName(appName)
        //.setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext: SQLContext = new SQLContext(sc)
        Alpha.alphaMiner(filePath = filePath,sqlContext=sqlContext)
        //println(filePath)

      case None =>
        // arguments are bad, error message will have been displayed
          //println("Need input filePath")
    }
  }
}
