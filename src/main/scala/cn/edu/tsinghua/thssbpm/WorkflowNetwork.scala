package cn.edu.tsinghua.thssbpm

import abstraction.{Event, Place}

class WorkflowNetwork(_eventList: Set[Event],
                      _workflowPlace: Set[Place],
                      _eventToPlaceTransitions: Set[Tuple2[Event, Place]],
                      _eventToPlaceTransitionsMap: Map[Event, Set[Place]],
                      _placeToEventTransitions: Set[Tuple2[Place, Event]],
                      _in: Place,
                      _out: Place) {
  private val eventList = _eventList
  private val workflowPlace = _workflowPlace
  private val eventToPlaceTransitions = _eventToPlaceTransitions
  private val eventToPlaceTransitionsMap = _eventToPlaceTransitionsMap
  private val placeToEventTransitions = _placeToEventTransitions
  private val in = _in
  private val out = _out

  def getWorkflowPlaces: Set[Place] = workflowPlace

  def getEventList: Set[Event] = eventList

  def getIn: Place = in

  def getOut: Place = out

  def getEventToPlaceTransitions: Set[Tuple2[Event, Place]] = eventToPlaceTransitions

  def getEventToPlaceTransitionsMap: Map[Event, Set[Place]] = eventToPlaceTransitionsMap

  def getPlaceToEventTransitions: Set[Tuple2[Place, Event]] = placeToEventTransitions

  def runTrace(trace:List[Event]):Boolean = {
    false
  }
}
