package cn.edu.tsinghua.thssbpm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class Config(filePath: String = null,
                  appName: String = "Flexible Heuristic Miner",
                  outputPath: String = null,
                  DeltaA: Double = 0.9,
                  DeltaL1L: Double = 0.9,
                  DeltaL2L: Double = 0.9,
                  DeltaLong: Double = 0.9,
                  DeltaRel: Double = 0.05
                 )

object Main {

  var parser = new scopt.OptionParser[Config]("scopt") {
    head("spark-fhm", "0.0.1")

    opt[String]('f', "filePath")
      .required()
      .action((x, c) => c.copy(filePath = x))
      .text("filePath is the event log file path")

    opt[String]('o', "outfilePath")
      .required()
      .action((x, c) => c.copy(outputPath = x))
      .text("outfilePath is the output file path")

    opt[Double]("DeltaA")
      .action((x, c) => c.copy(DeltaA = x))
      .text("argument: DeltaA")

    opt[Double]("DeltaL1L")
      .action((x, c) => c.copy(DeltaL1L = x))
      .text("argument: DeltaL1L")

    opt[Double]("DeltaL2L")
      .action((x, c) => c.copy(DeltaL2L = x))
      .text("argument: DeltaL2L")

    opt[Double]("DeltaLong")
      .action((x, c) => c.copy(DeltaLong = x))
      .text("argument: DeltaLong")

    opt[Double]("DeltaRel")
      .action((x, c) => c.copy(DeltaRel = x))
      .text("argument: DeltaRel")

    opt[String]("appName")
      .action((x, c) => c.copy(appName = x))
      .text("appName is the application name, default is 'FHM'")

  }

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        // do stuff
        val filePath = config.filePath
        val appName = config.appName
        val outputPath = config.outputPath
        val DeltaA = config.DeltaA
        val DeltaL1L = config.DeltaL1L
        val DeltaL2L = config.DeltaL2L
        val DeltaLong = config.DeltaLong
        val DeltaRel = config.DeltaRel


        //Set spark conf
        val conf = new SparkConf().setAppName(appName)
        //.setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext: SQLContext = new SQLContext(sc)
        FHM.fhm(filePath, outputPath, sqlContext, sc, DeltaA, DeltaL1L, DeltaL2L, DeltaLong, DeltaRel)
      case None =>
      // arguments are bad, error message will have been displayed
      //println("Need input filePath")
    }
  }


}
