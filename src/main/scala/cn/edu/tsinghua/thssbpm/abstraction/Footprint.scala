package cn.edu.tsinghua.thssbpm.abstraction

import scala.util.control.Breaks
import cn.edu.tsinghua.thssbpm.Util
import cn.edu.tsinghua.thssbpm.Util.Relation.{NoRelation, Parallel, ReversedSequence, Sequence}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.{Map => mMap}
import scala.Array.ofDim
import scala.collection.mutable

object Footprint {
  val eventNameToMatrixIndex: mutable.Map[String, Int] = mutable.Map[String, Int]()
  var footprint: Array[Array[Util.Relation.Value]] = Array[Array[Util.Relation.Value]]()
  var sortedEvents: List[Event] = List[Event]()
  val break = new Breaks
  override def toString: String = {
    var sb = new mutable.StringBuilder("\t")
    sortedEvents.foreach(event => sb.append(event.name + "\t"))
    sb.append("\n")
    for (i <- 0 until sortedEvents.length) {
      sb.append(sortedEvents(i).name + "\t")
      footprint(i).foreach(value => value match {
        case Sequence => sb.append("->\t")
        case Parallel => {
          sb.append("||\t")
        }
        case ReversedSequence => {
          sb.append("<-\t")
        }
        case NoRelation => {
          sb.append("#\t")
        }
      })
      sb.append("\n")
    }
    sb.toString()
  }

  /**
    * @param events    (Set[Event]) name of event set
    * @param traceList (DataFrame) name of trace list and it is a dataFrame data structure
    * @return footprint a two-dimensional array
    */
  def create(events: Set[Event], traceList: DataFrame): Array[Array[Util.Relation.Value]] = {
    val eventNum = events.size
    footprint = ofDim[Util.Relation.Value](eventNum, eventNum)
    for {i <- 0 until eventNum
         j <- 0 until eventNum}
      footprint(i)(j) = NoRelation
    sortedEvents = events.toList.sortBy(e => e.name)
    for (i <- 0 until eventNum)
      eventNameToMatrixIndex += (sortedEvents(i).name -> i)

    /**
      * Parse Direct Follows
      * We use a 2x2 Tuple2[Tuple2, Tuple2]:
      * (Event1:String, Event2:String) -> (Forward:Boolean, Backward:Boolean)
      * Forward means that if we have relation (Event1,Event2) or not
      * Backward means that if we have relation (Event2, Event1) or not
      * When parsing the traces, we have a trace like that ["Activity A","Activity B","Activity D"]
      * then:
      * ("Activity A","Activity B") -> (true, false)
      * ("Activity B","Activity D") -> (true, false)
      *
      * There are four situations:
      * ((Event1,Event2),(true,true)) => Event1 || Event2
      * ((Event1,Event2),(true,false)) => Event1 -> Event2
      * ((Event1,Event2),(false,true)) => Event1 <- Event2
      * ((Event1,Event2),(false,false)) => Event1 # Event2
      **/

    val followRelationDF = traceList.rdd.map(row => {
      val trace = row.getSeq[String](0).map(s => new Event(s))

      val directFollows = mMap[Tuple2[Event, Event], Tuple2[Boolean, Boolean]]()
      for {
        i <- 0 until trace.length - 1
        j <- i + 1 until trace.length
      } {
        directFollows += ((trace(i), trace(j)) -> (false, false))
      }
      for (i <- 0 to trace.length - 2) {
        if (eventNameToMatrixIndex(trace(i).name) < eventNameToMatrixIndex(trace(i + 1).name))
          directFollows((trace(i), trace(i + 1))) = (true, false)
        else
          directFollows((trace(i + 1), trace(i))) = (false, true)
      }
      directFollows.clone().toList
    })


    val relationMap = followRelationDF.flatMap(list => list.toSet)
      .reduceByKey((x, y) => ((x._1 || y._1), (x._2 || y._2)))
      .collect()

    /**
      * Generate footprint matrix
      * follows: ((event1,event2),(Boolean,Boolean))
      * eg:
      * if we have ((A,B),(true,false)),it means we have A->B and we don't have A<-B
      * if we have ((A,B),(false,true)),it means we have A<-B and we don't have A->B
      * if we have ((A,B),(true,true)), it means we have both A->B and A<-B, then => A||B
      * if we have ((A,B),(false,true)),it means A has no relation with B which means A#B
      */
    var forward: Util.Relation.Value = NoRelation
    var backward: Util.Relation.Value = NoRelation
    relationMap.foreach((follows: Tuple2[Tuple2[Event, Event], Tuple2[Boolean, Boolean]]) => {
      follows._2 match {
        case (false, false) => {
          forward = footprint(eventNameToMatrixIndex(follows._1._1.name))(eventNameToMatrixIndex(follows._1._2.name))
          backward = footprint(eventNameToMatrixIndex(follows._1._2.name))(eventNameToMatrixIndex(follows._1._1.name))
        }
        case (true, true) => {
          forward = Parallel
          backward = Parallel
        }
        case (false, true) => {
          forward = ReversedSequence
          backward = Sequence
        }
        case (true, false) => {
          forward = Sequence
          backward = ReversedSequence
        }
      }
      footprint(eventNameToMatrixIndex(follows._1._1.name))(eventNameToMatrixIndex(follows._1._2.name)) = forward
      footprint(eventNameToMatrixIndex(follows._1._2.name))(eventNameToMatrixIndex(follows._1._1.name)) = backward
    })
    footprint
  }

  def printFootPrint(): Unit = {
    println(footprint.toString)
  }

  def getRelationType(firstEvent: Event, secondEvent: Event): Util.Relation.Value = {
    val rowIndex: Int = eventNameToMatrixIndex(firstEvent.name)
    val colIndex: Int = eventNameToMatrixIndex(secondEvent.name)
    footprint(rowIndex)(colIndex)
  }

  /**
    * @param firstEvent  name of one event (i.e. a)
    * @param secondEvent name of another event (i.e. b)
    * @return false if a#b, true otherwise
    */
  def areConnected(firstEvent: Event, secondEvent: Event): Boolean = {
    getRelationType(firstEvent, secondEvent) != NoRelation
  }

  /**
    * @param firstEvent  name of one event (i.e. a)
    * @param secondEvent name of another event (i.e. b)
    * @return true if a>b, false otherwise
    */
  def isFirstFollowedBySecond(firstEvent: Event, secondEvent: Event): Boolean = {
    getRelationType(firstEvent, secondEvent) == Sequence
  }

  /**
    * @param inputEvents  first event set
    * @param outputEvents second event set
    * @return Boolean
    *         The function detect that if inputEvents and outputEvents are connected
    *         For inputEvents A and outputEvents B, we need to make sure that
    *         For every a1,a2 in A => a1#a2 or return false
    *         For every b1,b2 in B => b1#b2 or return false
    */
  def areEventsConnected(inputEvents: Set[Event], outputEvents: Set[Event]): Boolean = {
    // For every a1,a2 in A => a1#a2
    val inputEventsList = inputEvents.toList
    // TODO: make a functional code style!
    var areInputEventsConnectedBetweenThemselves = false
    break.breakable {
      for {inputI <- inputEventsList
           inputJ <- inputEventsList} {
        areInputEventsConnectedBetweenThemselves = areConnected(inputI, inputJ)
        if (areInputEventsConnectedBetweenThemselves)
          break.break
      }
    }
    if (areInputEventsConnectedBetweenThemselves)
      false
    else {
      //For every b1,b2 in B => b1#b2
      val outputEventsList = outputEvents.toList
      var areOutputEventsConnectedBetweenThemselves = false
      break.breakable {
        for {outputI <- outputEventsList
             outputJ <- outputEventsList} {
          areOutputEventsConnectedBetweenThemselves = areConnected(outputI, outputJ)
          if (areOutputEventsConnectedBetweenThemselves)
            break.break
        }
      }
      if (areOutputEventsConnectedBetweenThemselves)
        false
      else {
        var allFromBFollowAllFromA = true
        break.breakable {
          for {inputI <- inputEventsList
               outputJ <- outputEventsList} {
            allFromBFollowAllFromA = isFirstFollowedBySecond(inputI, outputJ)
            if (!allFromBFollowAllFromA)
              break.break
          }
        }
        allFromBFollowAllFromA
      }
    }
  }
}
