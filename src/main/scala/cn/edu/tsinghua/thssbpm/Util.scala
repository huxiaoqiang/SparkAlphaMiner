package cn.edu.tsinghua.thssbpm

import org.apache.spark.sql.{DataFrame, SQLContext}
import collection.mutable.{Set => mSet}
import abstraction.Event
object Util {

  /**
    * @author huxiaoqiang
    * @param originSet event set
    * @return Power set of originSet
    * eg:
    * If S is the set {x, y, z}, then the subsets of S are:
    *   {} (also denoted the empty set)
    *   {x},{y},{z}
    *   {x, y},{x, z},{y, z}
    *   {x, y, z}
    * the subset number of power set is power(2,n)
    */
  def powerSet(originSet: Set[Event]): Set[Set[Event]] = {
    var sets: mSet[Set[Event]] = mSet[Set[Event]]()
    if (originSet.isEmpty) {
      sets.add(Set[Event]())
    }
    else {
      val list: List[Event] = originSet.toList
      val head: Event = list.head
      val tail: collection.immutable.Set[Event] = list.tail.toSet
      powerSet(tail).foreach(set => {
        var newSet: mSet[Event] = mSet[Event]()
        newSet.add(head)
        set.foreach(s => newSet.add(s))
        sets.add(newSet.toSet)
        sets.add(set)
      })
    }
    sets.toSet
  }

  def parseXesLogFromHDFS(sqlContext: SQLContext, filePath: String): DataFrame = {
    var df: DataFrame = null

    df = sqlContext.read
      .format("xml")
      .option("rowTag", "trace")
      .load(filePath)

    val traceList = df.select("event") //get trace list
      .take(df.count().toInt) //take all trace list as Row list
      .map(t => t.toString().split("concept:name,").tail.map(s => s.split("]")(0)))

    import sqlContext.implicits._
    traceList.toSeq.toDF("trace")
  }

  def parseXesLogFromHDFSToLocal(sqlContext: SQLContext, filePath: String): List[List[Event]] = {
    var df: DataFrame = null

    df = sqlContext.read
      .format("xml")
      .option("rowTag", "trace")
      .load(filePath)

    val traceList = df.select("event") //get trace list
      .take(df.count().toInt) //take all trace list as Row list
      .map(t => t.toString().split("concept:name,").tail.map(s => s.split("]")(0)))

    val castTraceList = traceList.map(t => t.map(e => new Event(e)).toList).toList
    castTraceList
  }

  /**
  * event four relation
  * Sequence: ->
  * ReversedSequence: <-
  * Parallel: ||
  * NoRelation: #
  **/
  object Relation extends Enumeration {
    type Relation = Value
    val Sequence = Value(0)
    val ReversedSequence = Value(1)
    val Parallel = Value(2)
    val NoRelation = Value(3)
  }

}
