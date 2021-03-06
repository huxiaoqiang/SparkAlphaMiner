package cn.edu.tsinghua.thssbpm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class Config(filePath:String=null,
                  appName:String="alpha miner",
                  outputPath:String=null)

object Main {

  var parser = new scopt.OptionParser[Config]("scopt") {
    head("spark-alpha","0.0.1")

    opt[String]('f',"filePath")
      .required()
      .action((x,c)=>c.copy(filePath=x))
      .text("filePath is the event log file path")

    opt[String]('o',"outfilePath")
      .required()
      .action((x,c)=>c.copy(outputPath=x))
      .text("outfilePath is the output file path")

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
        val outputPath = config.outputPath

        //Set spark conf
        val conf = new SparkConf().setAppName(appName)
        //.setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext: SQLContext = new SQLContext(sc)
        Alpha.alphaMiner(filePath = filePath,outputPath=outputPath, sqlContext=sqlContext,sc=sc)

      case None =>
        // arguments are bad, error message will have been displayed
          //println("Need input filePath")
    }
  }


}
