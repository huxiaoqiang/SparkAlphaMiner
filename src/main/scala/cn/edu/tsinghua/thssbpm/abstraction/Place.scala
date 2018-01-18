package cn.edu.tsinghua.thssbpm.abstraction

import collection.mutable.{Set => mSet}

class Place(in: Set[Event], out: Set[Event], n: String = "") {

  val name: String = if (n != "") n else {
    Place.placeName += 1; "place" + Place.placeName.toString
  }
  private val inEventsSet: mSet[Event] = mSet[Event]() ++ in
  private val outEventsSet: mSet[Event] = mSet[Event]() ++ out

  override def toString: String = name

  def getInEvents: Set[Event] = inEventsSet.toSet

  def getOutEvents: Set[Event] = outEventsSet.toSet

  def getName: String = name

  def addOutEvent(event: Event): Unit = outEventsSet.add(event)

  def addInEvent(event: Event): Unit = inEventsSet.add(event)
}

object Place {
  var placeName = 0
}
