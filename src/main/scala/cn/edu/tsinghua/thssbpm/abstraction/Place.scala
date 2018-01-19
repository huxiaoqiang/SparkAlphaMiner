package cn.edu.tsinghua.thssbpm.abstraction

import collection.mutable.{Set => mSet}

@SerialVersionUID(1000L)
class Place(in: Set[Event], out: Set[Event], n: String = "") extends Serializable {

  val name: String = if (n != "") n else {
    Place.placeName += 1
    "place" + Place.placeName.toString
  }
  private val inEventsSet: mSet[Event] = mSet[Event]() ++ in
  private val outEventsSet: mSet[Event] = mSet[Event]() ++ out
  private var token = false

  override def toString: String = name

  def getInEvents: Set[Event] = inEventsSet.toSet

  def getOutEvents: Set[Event] = outEventsSet.toSet

  def getName: String = name

  def addOutEvent(event: Event): Unit = outEventsSet.add(event)

  def addInEvent(event: Event): Unit = inEventsSet.add(event)

  def hasToken: Boolean = token


  def putToken(): Unit = {
    token = true
  }

  def takeToken(): Unit = {
    token = false
  }

  def clearToken(): Unit = {
    token = false
  }

  def canEqual(p: Any) = p.isInstanceOf[Place]

  override def equals(that: scala.Any): Boolean = that match {
    case that: Place => {
      if (that == null && this != null)
        false
      else if (that != null && this == null)
        false
      else
        that.canEqual(this) && this.hashCode() == that.hashCode()
    }
    case _ => false
  }

  override def hashCode(): Int = {
    val prime: Int = 31
    val r: Int = if (inEventsSet.isEmpty && outEventsSet.isEmpty) 0 else inEventsSet.hashCode() + outEventsSet.hashCode()
    r + prime
  }
}

object Place {
  var placeName = 0
}
