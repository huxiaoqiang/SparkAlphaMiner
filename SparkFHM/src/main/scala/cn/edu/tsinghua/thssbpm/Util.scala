package cn.edu.tsinghua.thssbpm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Util {

  def parseTextLogHDFS(sc: SparkContext, filePath: String): RDD[List[String]] = {
    val tableLog = sc.textFile(filePath)
    val trace = tableLog.map(line => {
      val l = line.split("\t")
      (l(0),l(1))
    })
    trace.groupByKey().values.map[List[String]](it=>it.toList)
  }


  /**
    * event four relation
    * DIRECT: >
    * LOOPLENGTHTOW: >>
    * SUCCESSOR: >>>
    * COUNT: count
    **/

  object Relation extends Enumeration {
    type Relation = Value
    val DIRECT = Value(0)
    val LOOPLENGTHTOW = Value(1)
    val SUCCESSOR = Value(2)
    val COUNT = Value(3)
  }

}
