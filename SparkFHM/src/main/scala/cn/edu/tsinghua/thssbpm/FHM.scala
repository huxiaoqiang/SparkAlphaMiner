package cn.edu.tsinghua.thssbpm

import cn.edu.tsinghua.thssbpm.Util.Relation
import cn.edu.tsinghua.thssbpm.abstraction.{DependencyRelation, Event}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.{Map => mMap}
import scala.util.control.Breaks

object FHM {
  def fhm(filePath: String,
          outputPath: String,
          sqlContext: SQLContext,
          sc: SparkContext,
          DeltaA: Double,
          DeltaL1L: Double,
          DeltaL2L: Double,
          DeltaLong: Double,
          DeltaRel: Double): Unit = {
    //read the logs
    val traceList = Util.parseTextLogHDFS(sc, filePath)
    traceList.persist(StorageLevel.MEMORY_AND_DISK)
    val collectedSortedEvents = traceList
      .flatMap(r => r.map(r => (r, 1)))
      .reduceByKey((i1, i2) => 1)
      .collect()
      .map(a => new Event(a._1))
      .toList.sortBy(e => e.name)

    val eventNameToMatrixIndex: mutable.Map[Event, Int] = mutable.Map[Event, Int]()
    val matrixIndexToEventName: mutable.Map[Int, Event] = mutable.Map[Int, Event]()
    for (i <- 0 until collectedSortedEvents.length) {
      eventNameToMatrixIndex += (collectedSortedEvents(i) -> i)
      matrixIndexToEventName += (i -> collectedSortedEvents(i))
    }
    val eventNameToMatrixIndexBC = sc.broadcast(eventNameToMatrixIndex)
    val matrixIndexToEventNameBC = sc.broadcast(matrixIndexToEventName)

    //get dependency
    val dependencyRelation = traceList.flatMap(tl => {
      val trace = tl.map(s => new Event(s))
      val dependency = mMap[DependencyRelation, Tuple2[Double, Double]]()
      //add |a|
      for (i <- 0 until trace.length)
        dependency += ((new DependencyRelation(trace(i), trace(i), Relation.COUNT.toString())) -> (1.0, 0.0))
      //add a>b
      for (i <- 0 until trace.length - 1) {
        if (eventNameToMatrixIndexBC.value(trace(i)) <= eventNameToMatrixIndexBC.value(trace(i + 1)))
          dependency += ((new DependencyRelation(trace(i), trace(i + 1), Relation.DIRECT.toString())) -> (1.0, 0.0))
        else
          dependency += ((new DependencyRelation(trace(i + 1), trace(i), Relation.DIRECT.toString())) -> (0.0, 1.0))
      }
      //add a>>b
      for (i <- 0 until trace.length - 2) {
        if (trace(i) == trace(i + 2) && trace(i + 2) != trace(i + 1)) {
          if (eventNameToMatrixIndexBC.value(trace(i)) < eventNameToMatrixIndexBC.value(trace(i + 1)))
            dependency += ((new DependencyRelation(trace(i), trace(i + 1), Relation.LOOPLENGTHTOW.toString())) -> (1.0, 0.0))
          else
            dependency += ((new DependencyRelation(trace(i + 1), trace(i), Relation.LOOPLENGTHTOW.toString())) -> (0.0, 1.0))
        }
      }
      //add a>>>b
      for (i <- 0 until trace.length - 1) {
        for (j <- i + 1 until trace.length) {
          if (eventNameToMatrixIndexBC.value(trace(i)) <= eventNameToMatrixIndexBC.value(trace(j)))
            dependency += ((new DependencyRelation(trace(i), trace(j), Relation.SUCCESSOR.toString())) -> (1.0, 0.0))
          else
            dependency += ((new DependencyRelation(trace(j), trace(i), Relation.SUCCESSOR.toString())) -> (0.0, 1.0))
        }
      }
      dependency.clone().toList
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val count = dependencyRelation
      .filter(_._1.getRelationType == Relation.COUNT.toString)
      .map(relationMap => (relationMap._1.getEvent1, relationMap._2._1)).collect().toMap
    val countBC = sc.broadcast(count)

    /* dependencyRelation is as folow:
    * (a, b, direct), (c1,c2): c1 means times of a>b,c2 means times of b>a
    */
    val dependencyMeasure = dependencyRelation.filter(_._1.getRelationType != Relation.COUNT.toString)
      .map(relationMap => {
        var forward: Double = 0.0
        var backward: Double = 0.0
        relationMap._1.getRelationType match {
          case "DIRECT" => {
            if (relationMap._1.getEvent1 == relationMap._1.getEvent2) {
              forward = relationMap._2._1 / (relationMap._2._1 + 1)
              backward = 0.0
            }
            else {
              forward = (relationMap._2._1 - relationMap._2._2) / (relationMap._2._1 + relationMap._2._2 + 1)
              backward = (relationMap._2._2 - relationMap._2._1) / (relationMap._2._1 + relationMap._2._2 + 1)
            }
          }
          case "LOOPLENGTHTOW" => {
            forward = (relationMap._2._1 - relationMap._2._2) / (relationMap._2._1 + relationMap._2._2 + 1)
            backward = (relationMap._2._2 - relationMap._2._1) / (relationMap._2._1 + relationMap._2._2 + 1)
          }
          case "SUCCESSOR" => {
            val countA = countBC.value(relationMap._1.getEvent1)
            val countB = countBC.value(relationMap._1.getEvent2)
            forward = 2.0 * (relationMap._2._1 - math.abs(countA - countB)) / (countA + countB + 1)
            backward = 2.0 * (relationMap._2._2 - math.abs(countB - countA)) / (countA + countB + 1)
          }
        }
        (relationMap._1, (forward, backward))
      })


    val dependencyMeasureNew = dependencyMeasure.flatMap(measure => {
      List((measure._1.getEvent1, measure._1.getEvent2, measure._1.getRelationType, measure._2._1),
        (measure._1.getEvent2, measure._1.getEvent1, measure._1.getRelationType, measure._2._2))
    })
    val directMap = dependencyMeasureNew
      //measure._4 >0  deal with loop length one like "GG", we collect it before (G,G,1000,0)
      .filter(measure => {
      measure._3 == Relation.DIRECT.toString && measure._4 > 0
    })
      .map(measure => ((measure._1, measure._2), measure._4))
      .collect().toMap

    val l2lMap = dependencyMeasureNew
      .filter(measure => {
        measure._3 == Relation.LOOPLENGTHTOW.toString && measure._4 > 0
      })
      .map(measure => ((measure._1, measure._2), measure._4))
      .collect().toMap

    val succMap = dependencyMeasureNew
      .filter(measure => {
        measure._3 == Relation.SUCCESSOR.toString && measure._4 > 0
      })
      .map(measure => ((measure._1, measure._2), measure._4))
      .collect().toMap

    //step 2: get C1
    val C1: mutable.Map[Event, Event] = mutable.Map[Event, Event]()
    for ((k, v) <- directMap) {
      if (k._1 == k._2 && v >= DeltaL1L) {
        C1 += k
      }
    }

    //step 3: construct C2
    val C2: mutable.Map[Event, Event] = mutable.Map[Event, Event]()
    for ((k, v) <- l2lMap) {
      if (!C1.contains(k._1) && !C1.contains(k._2) && v >= DeltaL2L) {
        C2 += k
      }
    }

    //step 4: construct Cout
    val Cout: mutable.Map[Event, Event] = mutable.Map[Event, Event]()
    for (i <- 0 until collectedSortedEvents.length) {
      var maxVal = Double.MinValue
      val maxIndex = mutable.Set[Int]()
      val eventI = matrixIndexToEventName(i)
      for (j <- 0 until collectedSortedEvents.length) {
        if (i != j) {
          val eventJ = matrixIndexToEventName(j)
          if (directMap.contains((eventI, eventJ))) {
            if (directMap((eventI, eventJ)) > maxVal) {
              maxVal = directMap((eventI, eventJ))
              maxIndex.clear()
              maxIndex.add(j)
            }
            else if (directMap((eventI, eventJ)) == maxVal) {
              maxIndex.add(j)
            }
          }
        }
      }
      for (j <- maxIndex) {
        Cout += (eventI -> matrixIndexToEventName(j))
      }
    }

    //step5: construct Cin
    val Cin: mutable.Map[Event, Event] = mutable.Map[Event, Event]()
    for (i <- 0 until collectedSortedEvents.length) {
      var maxVal = Double.MinValue
      val maxIndex = mutable.Set[Int]()
      val eventI = matrixIndexToEventName(i)
      for (j <- 0 until collectedSortedEvents.length) {
        if (i != j) {
          val eventJ = matrixIndexToEventName(j)
          if (directMap.contains((eventJ, eventI))) {
            if (directMap((eventJ, eventI)) > maxVal) {
              maxVal = directMap((eventJ, eventI))
              maxIndex.clear()
              maxIndex.add(j)
            }
            else if (directMap((eventJ, eventI)) == maxVal) {
              maxIndex.add(j)
            }
          }
        }
      }
      for (j <- maxIndex) {
        Cout += (matrixIndexToEventName(j) -> eventI)
      }
    }

    //step6: construct CoutPrime
    val CoutPrime: mutable.Set[(Event, Event)] = mutable.Set[(Event, Event)]()
    for ((axEvt1, axEvt2) <- Cout) {
      if (directMap.contains((axEvt1, axEvt2)) && directMap((axEvt1, axEvt2)) < DeltaA) {

        for ((byEvt1, byEvt2) <- Cout) {
          val break = new Breaks
          break.breakable {
            for ((abEvt1, abEvt2) <- C2) {
              if (axEvt1 == abEvt1 && byEvt1 == abEvt2
                && directMap.contains((byEvt1, byEvt2))
                && directMap.contains((axEvt1, axEvt2))
                && directMap((byEvt1, byEvt2)) - directMap((axEvt1, axEvt2)) > DeltaRel) {
                CoutPrime += (axEvt1 -> axEvt2)
                break.break()
              }
            }
          }
        }
      }
    }

    //step7:Remove CoutPrime from Cout
    for ((k, v) <- CoutPrime) {
      Cout.remove(k)
    }

    //step8: Construct CinPrime
    val CinPrime: mutable.Set[(Event, Event)] = mutable.Set[(Event, Event)]()
    for ((axEvt1, axEvt2) <- Cin) {
      if (directMap.contains((axEvt1, axEvt2)) && directMap((axEvt1, axEvt2)) < DeltaA) {

        for ((byEvt1, byEvt2) <- Cin) {
          val break = new Breaks
          break.breakable {
            for ((abEvt1, abEvt2) <- C2) {
              if (axEvt2 == abEvt1 && byEvt2 == abEvt2
                && directMap.contains((byEvt1, byEvt2))
                && directMap.contains((axEvt1, axEvt2))
                && directMap((byEvt1, byEvt2)) - directMap((axEvt1, axEvt2)) > DeltaRel) {
                CinPrime += (axEvt1 -> axEvt2)
                break.break()
              }
            }
          }
        }
      }
    }

    //step9:Remove CInPrime from CIn
    for ((k, v) <- CinPrime) {
      Cin.remove(k)
    }

    //step10: Construct CoutPP
    val CoutPP: mutable.Set[(Event, Event)] = mutable.Set[(Event, Event)]()
    for (i <- 0 until collectedSortedEvents.length) {
      for (j <- 0 until collectedSortedEvents.length) {
        val eventI = matrixIndexToEventName(i)
        val eventJ = matrixIndexToEventName(j)
        if (directMap.contains((eventI, eventJ)) && directMap((eventI, eventJ)) >= DeltaA) {
          CoutPP.add((eventI -> eventJ))
        }
        val break = new Breaks
        break.breakable {
          for ((evt1, evt2) <- Cout) {
            if (evt1 == eventI
              && directMap.contains((evt1, evt2))
              && directMap.contains((eventI, eventJ))
              && directMap((evt1, evt2)) - directMap((eventI, eventJ)) < DeltaRel) {
              CoutPP.add((eventI -> eventJ))
              break.break()
            }
          }
        }
      }
    }

    //step11:Construct CinPP
    val CinPP: mutable.Set[(Event, Event)] = mutable.Set[(Event, Event)]()
    for (i <- 0 until collectedSortedEvents.length) {
      for (j <- 0 until collectedSortedEvents.length) {
        val eventI = matrixIndexToEventName(i)
        val eventJ = matrixIndexToEventName(j)
        if (directMap.contains((eventI, eventJ)) && directMap((eventI, eventJ)) >= DeltaA) {
          CinPP.add((eventI -> eventJ))
        }
        val break = new Breaks
        break.breakable {
          for ((evt1, evt2) <- Cout) {
            if (evt1 == eventI
              && directMap.contains((evt1, evt2))
              && directMap.contains((eventI, eventJ))
              && directMap((evt1, evt2)) - directMap((eventI, eventJ)) < DeltaRel) {
              CoutPP.add((eventI -> eventJ))
              break.break()
            }
          }
        }
      }
    }

    //step: Construct Dependence Graph DG
    val DG: mutable.Set[(Event, Event)] = mutable.Set[(Event, Event)]()
    DG ++= C1
    DG ++= C2
    DG ++= Cin
    DG ++= Cout
    DG ++= CoutPP
    DG ++= CinPP

    //Deal with Long Distance Dependences
    for ((k, v) <- succMap) {
      if (v > DeltaLong)
        DG += k
    }


    val succ: mutable.Map[String, mutable.Set[String]] = mutable.Map[String, mutable.Set[String]]()
    val pred: mutable.Map[String, mutable.Set[String]] = mutable.Map[String, mutable.Set[String]]()

    for ((ev1, ev2) <- DG) {
      if (succ.contains(ev1.name))
        succ(ev1.name).add(ev2.name)
      else
        succ += (ev1.name -> mutable.Set[String](ev2.name))

      if (pred.contains(ev2.name))
        pred(ev2.name).add(ev1.name)
      else
        pred += (ev2.name -> mutable.Set[String](ev1.name))
    }

    val succBC = sc.broadcast(succ)
    val predBC = sc.broadcast(pred)
    val cNet = traceList.flatMap(list => {
      val CNetArgs: mutable.Map[(String, mutable.Set[String]), Int] = mutable.Map[(String, mutable.Set[String]), Int]()
      for (i <- 0 until list.length - 1) {
        val currEvent = list(i)
        val candidates: mutable.Set[String] = mutable.Set[String]()
        val rest: List[String] = list.takeRight(list.length - i - 1)
        if (succBC.value.contains(currEvent)) {
          val dgEventSucc = succBC.value(currEvent)
          for (s <- dgEventSucc) {
            if (rest.contains(s)) {
              val succIndex = rest.indexOf(s)
              val preds = predBC.value(s)
              val restPreds = rest.take(succIndex)
              var needAdd = true
              for (pred <- preds) {
                if (restPreds.contains(pred))
                  needAdd = false
              }
              if (needAdd)
                candidates.add(s)
            }
          }
          if (!candidates.isEmpty) {
            CNetArgs += (currEvent + "/SUCC", candidates) -> 1
          }
        }
      }

      for (i <- 1 until list.length) {
        val currEvent = list(i)
        val candidates: mutable.Set[String] = mutable.Set[String]()
        val rest: List[String] = list.take(i)
        if (predBC.value.contains(currEvent)) {
          val dgEventPred = predBC.value(currEvent)
          for (s <- dgEventPred) {
            if (rest.contains(s)) {
              val predIndex = rest.lastIndexOf(s)
              val succs = succBC.value(s)
              val restSuccs = rest.takeRight(rest.size - predIndex - 1)
              var needAdd = true
              for (succ <- succs) {
                if (restSuccs.contains(succ))
                  needAdd = false
              }
              if (needAdd)
                candidates.add(s)
            }
          }
          if (!candidates.isEmpty) {
            CNetArgs += (currEvent + "/PRED", candidates) -> 1
          }
        }
      }
      CNetArgs.clone().toList
    })

    val CNetReduce = cNet.reduceByKey(_ + _)
      .repartition(1)
    //      .collect()
    //    for(i <- CNetReduce)
    //      println(i._1->i._2)

    CNetReduce.saveAsTextFile(outputPath)
    println("done")
  }
}