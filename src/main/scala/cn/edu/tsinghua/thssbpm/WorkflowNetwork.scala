package cn.edu.tsinghua.thssbpm

import abstraction.{Event, Place}

import collection.mutable.{Map => mMap}
import scala.util.control.Breaks

@SerialVersionUID(1000L)
class WorkflowNetwork(_eventList: Set[Event],
                      _workflowPlace: Set[Place],
                      _eventToPlaceTransitions: Set[Tuple2[Event, Place]],
                      _eventToPlaceTransitionsMap: Map[Event, Set[Place]],
                      _placeToEventTransitions: Set[Tuple2[Place, Event]],
                      _in: Place,
                      _out: Place) extends Serializable{
  private val eventList = _eventList
  private val workflowPlace = _workflowPlace
  private val eventToPlaceTransitions = _eventToPlaceTransitions
  private val eventToPlaceTransitionsMap = _eventToPlaceTransitionsMap
  private val placeToEventTransitions = _placeToEventTransitions
  private val in = _in
  private val out = _out
  private val eventPrePostMap = this.createActivityPrePostMap(workflowPlace,eventList)

  def getWorkflowPlaces: Set[Place] = workflowPlace

  def getEventList: Set[Event] = eventList

  def getIn: Place = in

  def getOut: Place = out

  def getEventToPlaceTransitions: Set[Tuple2[Event, Place]] = eventToPlaceTransitions

  def getEventToPlaceTransitionsMap: Map[Event, Set[Place]] = eventToPlaceTransitionsMap

  def getPlaceToEventTransitions: Set[Tuple2[Place, Event]] = placeToEventTransitions

  def createActivityPrePostMap(workflowPlace:Set[Place],eventList:Set[Event]):Map[Event, Tuple2[Set[Place],Set[Place]]] = {
    val eventPrePostMap:mMap[Event, Tuple2[Set[Place],Set[Place]]] = mMap[Event, Tuple2[Set[Place],Set[Place]]]()
    for(e <- eventList){
      val inPlace = workflowPlace.filter(p=>p.getInEvents.contains(e))
      val outPlace = workflowPlace.filter(p=>p.getOutEvents.contains(e))
      eventPrePostMap ++= Map(e -> (inPlace,outPlace))
    }
    eventPrePostMap.toMap
  }

  def runTrace(trace:List[Event]):Boolean = {
    this.workflowPlace.foreach(p=>p.clearToken())
    in.putToken()
    val break = new Breaks
    var result = false
    break.breakable{
      for(currentEvent <- eventList){
        if(out.hasToken) {
          result = false
          break.break()
        }
        val actPrePost = eventPrePostMap(currentEvent)
        if(actPrePost == null){
          result = false
          break.break()
        }
        val inPlace:Set[Place] = actPrePost._1
        //var enable = true
        val enable = inPlace.filter(p => {!p.hasToken}).isEmpty

        if(enable){
          inPlace.foreach(p=>p.takeToken())
          actPrePost._2.foreach(p=>p.putToken())
        }
      }
    }

    if(!out.hasToken || result == false)
       false
    else
      workflowPlace.filter(p=>p.hasToken).size == 1
  }
}
