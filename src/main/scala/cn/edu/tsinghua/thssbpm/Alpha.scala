package cn.edu.tsinghua.thssbpm

import org.apache.spark.sql.SQLContext

import scala.collection.mutable.{Map => mMap, Set => mSet}
import abstraction.{Event, Footprint, Place}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
* SparkAlpha Miner
* @author huxiaoqiang
*/

object Alpha {

  def alphaMiner(filePath:String,outputPath:String,sqlContext:SQLContext,sc:SparkContext): Unit = {
    //read the logs
    val traceList = Util.parseTextLogHDFS(sc, filePath)
    traceList.persist(StorageLevel.MEMORY_AND_DISK)

    val collectedSortedEvents = traceList
      .flatMap(r=>r.map(r=>(r,1)))
      .reduceByKey((i1,i2)=>1)
      .collect()
      .map(a=> new Event(a._1))


    //step 1,2,3 get Tw,Ti,Tox
    val events = collectedSortedEvents.toSet
    //val startingEventsRDD = traceListRDD.map(trace=>trace.eventList.head)
    val startingEvents = traceList.map(s => s.head)
      .distinct()
      .collect()
      .toList
      .map(e => new Event(e))
      .toSet

    val endingEvents = traceList.map(s => s.last)
      .distinct()
      .collect()
      .toList
      .map(e => new Event(e))
      .toSet
//
//    //Get footprint matrix
    val footprint = Footprint.create(events, traceList, sc)
    

    val outRdd = sc.parallelize(Footprint.toString)
    outRdd.repartition(1).saveAsTextFile(outputPath)
    println("footprint matrix is as follow: ")
    println(Footprint.toString)
    //Step 4 generate places
//    println("Getting places from footprint matrix")
//    val xl = getPlacesFromFootprint(footprint, events)
//
//    startTime = System.nanoTime()
//    //Step 5 reduce xl to get workflow places
//    println("reduce xl to get workflow places")
//    val workflowPlace: mSet[Place] = reducePlaces(xl)
//    endTime =System.nanoTime()
//
//    println("Reduce places taking time: " + (endTime - startTime)/(60*1000000.0) + "min" )
//
//    //Step 7: Create Transitions
//    val (eventToPlaceTransitions, eventToPlaceTransitionsMap) = createEventToPlaceTransitions(events, workflowPlace.toSet)
//    val placeToEventTransitions = createPlaceToEventTransitions(events, workflowPlace.toSet)
//
//    //Step 6: Source place and Sink place
//    val in: Place = new Place(Set[Event](), Set[Event](), "In")
//    val out: Place = new Place(Set[Event](), Set[Event](), "Out")
//    connectSourceAndSink(in, out, startingEvents, endingEvents, workflowPlace, placeToEventTransitions, eventToPlaceTransitions)
//    val workflowNetwork: WorkflowNetwork = new WorkflowNetwork(events, workflowPlace.toSet, eventToPlaceTransitions.toSet, eventToPlaceTransitionsMap.toMap, placeToEventTransitions.toSet, in, out)
//    println("done")
//    var correctRunTrace = 0
//    var errorRunTrace = 0
//    traceList.rdd.map(row => {
//      if(workflowNetwork.runTrace(row.getSeq[String](0).map(s => new Event(s)).toList))
//        correctRunTrace+=1
//      else
//        errorRunTrace+=1
//    })
//    println(correctRunTrace.toFloat/(correctRunTrace+errorRunTrace))
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
