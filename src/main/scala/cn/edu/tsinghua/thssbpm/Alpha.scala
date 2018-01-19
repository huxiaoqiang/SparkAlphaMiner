package cn.edu.tsinghua.thssbpm

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.{Map => mMap, Set => mSet}
import abstraction.{Event, Footprint, Place, Trace}
import org.apache.spark.rdd.RDD

/**
* SparkAlpha Miner
* @author huxiaoqiang
*/


object Alpha {

  //val filePath = "hdfs://192.168.3.200:9000/spark/Demo/logs/test.xes"

  def alphaMiner(filePath:String,sqlContext:SQLContext): Unit = {

    //Read .xes log file to trace string list
    val traceListRDD: RDD[Trace] = Util.parseXesLogFromHDFSToRDD(sqlContext, filePath)

    val traceList = Util.parseXesLogFromHDFS(sqlContext, filePath)

    val sortedEventsRDD = traceListRDD.flatMap(trace => trace.eventList)
      .distinct()
      .sortBy(event=>event.name)

    val collectedSortedEvents = sortedEventsRDD.collect()
//      traceList.rdd
//      .flatMap(r => r.getSeq[String](0))
//      .distinct()
//      .collect()
//      .sorted
//      .map(e => new Event(e))


    //step 1,2,3 get Tw,Ti,To
    val events = collectedSortedEvents.toSet
    //val startingEventsRDD = traceListRDD.map(trace=>trace.eventList.head)
    val startingEvents = traceList.rdd.map(s => {
      s.getSeq[String](0).head
    })
      .distinct()
      .collect()
      .toList
      .map(e => new Event(e))
      .toSet

    val endingEvents = traceList.rdd.map(s => {
      s.getSeq[String](0).last
    })
      .distinct()
      .collect()
      .toList
      .map(e => new Event(e))
      .toSet

    //Get footprint matrix
    val footprint = Footprint.create(events, traceList)
    println("footprint matrix is as follow: ")
    println(Footprint.toString)
    //Step 4 generate places
    println("Getting places from footprint matrix")
    val xl = getPlacesFromFootprint(footprint, events)

    //Step 5 reduce xl to get workflow places
    println("reduce xl to get workflow places")
    val workflowPlace: mSet[Place] = reducePlaces(xl)

    //Step 7: Create Transitions
    val (eventToPlaceTransitions, eventToPlaceTransitionsMap) = createEventToPlaceTransitions(events, workflowPlace.toSet)
    val placeToEventTransitions = createPlaceToEventTransitions(events, workflowPlace.toSet)

    //Step 6: Source place and Sink place
    val in: Place = new Place(Set[Event](), Set[Event](), "In")
    val out: Place = new Place(Set[Event](), Set[Event](), "Out")
    connectSourceAndSink(in, out, startingEvents, endingEvents, workflowPlace, placeToEventTransitions, eventToPlaceTransitions)
    val workflowNetwork: WorkflowNetwork = new WorkflowNetwork(events, workflowPlace.toSet, eventToPlaceTransitions.toSet, eventToPlaceTransitionsMap.toMap, placeToEventTransitions.toSet, in, out)

    var correctRunTrace = 0
    var errorRunTrace = 0
    traceList.rdd.map(row => {
      if(workflowNetwork.runTrace(row.getSeq[String](0).map(s => new Event(s)).toList))
        correctRunTrace+=1
      else
        errorRunTrace+=1
    })
    println(correctRunTrace.toFloat/(correctRunTrace+errorRunTrace))
  }

  def getPlacesFromFootprint(footPrint: Array[Array[Util.Relation.Value]],
                             events: Set[Event]): Set[Place] = {
    var xl: Set[Place] = Set[Place]()
    val powerSet = Util.powerSet(events).filter(set => set.nonEmpty)
    println(s"Get powerSet size ${powerSet.size}")
    val powerSetArray = powerSet.toArray
    for (i <- 0 until powerSetArray.length) {
      val first = powerSetArray(i)
      for (j <- 0 until powerSetArray.length) {
        j - i match {
          case 0 => {}
          case _ => {
            val second = powerSetArray(j)
            if (Footprint.areEventsConnected(first, second))
              xl += new Place(first, second)
          }
        }
      }
    }
    xl
  }

  def reducePlaces(xl: Set[Place]): mSet[Place] = {
    var yl: mSet[Place] = mSet[Place]() ++ xl
    var removedSetIndex: mSet[Int] = mSet[Int]()

    val potentialPlaces = xl.toList
    for (i <- 0 until potentialPlaces.length - 1) {
      val placeI = potentialPlaces(i)
      for (j <- i + 1 until potentialPlaces.length) {
        val placeJ = potentialPlaces(j)
        if (placeI.getInEvents.subsetOf(placeJ.getInEvents)
          && placeI.getOutEvents.subsetOf(placeJ.getOutEvents)) {
          removedSetIndex.add(i)
        }

        if (placeJ.getInEvents.subsetOf(placeI.getInEvents)
          && placeJ.getOutEvents.subsetOf(placeI.getOutEvents)) {
          removedSetIndex.add(j)
        }
      }
    }
    removedSetIndex.foreach(i => yl.remove(potentialPlaces(i)))
    yl
  }

  def createEventToPlaceTransitions(events: Set[Event], workflowPlace: Set[Place]): (mSet[Tuple2[Event, Place]], mMap[Event, Set[Place]]) = {
    val eventToPlaceTransitions: mSet[Tuple2[Event, Place]] = mSet()
    val eventToPlaceTransitionsMap: mMap[Event, Set[Place]] = mMap()
    events.foreach(e => {
      val eventToPlace: mSet[Place] = mSet()
      workflowPlace
        .filter(place => place.getInEvents.contains(e))
        .foreach(p => {
          eventToPlaceTransitions.add((e, p))
          eventToPlace.add(p)
        })
      eventToPlaceTransitionsMap += (e -> eventToPlace.toSet)
    })
    (eventToPlaceTransitions, eventToPlaceTransitionsMap)
  }

  def createPlaceToEventTransitions(events: Set[Event], workflowPlace: Set[Place]) = {
    val placeToEventTransitions: mSet[Tuple2[Place, Event]] = mSet()
    events.foreach(event => {
      workflowPlace
        .filter(place => place.getOutEvents.contains(event))
        .map(place => (place, event))
        .foreach(placeToEventTransition => placeToEventTransitions.add(placeToEventTransition))
    })
    placeToEventTransitions
  }

  def connectSourceAndSink(in: Place, out: Place,
                           startingEvents: Set[Event],
                           endingEvents: Set[Event],
                           workflowPlace: mSet[Place],
                           placeToEventTransitions: mSet[Tuple2[Place, Event]],
                           eventToPlaceTransitions: mSet[Tuple2[Event, Place]]): Unit = {
    startingEvents.foreach(sEvent => {
      in.addOutEvent(sEvent)
      placeToEventTransitions.add(Tuple2(in, sEvent))
    })

    endingEvents.foreach(eEvent => {
      out.addInEvent(eEvent)
      eventToPlaceTransitions.add(Tuple2(eEvent, out))
    })
    workflowPlace.add(in)
    workflowPlace.add(out)
  }
}
