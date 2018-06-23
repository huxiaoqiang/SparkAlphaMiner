package cn.edu.tsinghua.thssbpm.abstraction

import cn.edu.tsinghua.thssbpm.Util.Relation

@SerialVersionUID(1000L)
class DependencyRelation(ev1: Event, ev2: Event, rt: String) extends Serializable {
  val event1 = ev1
  val event2 = ev2
  val relationType = rt

  def getRelationType = relationType

  def getEvent1 = event1

  def getEvent2 = event2

  override def toString: String = "<%s,%s>:%s".format(event1, event2, relationType)

  def canEqual(e: Any) = e.isInstanceOf[DependencyRelation]

  override def equals(that: scala.Any): Boolean = that match {
    case that: DependencyRelation => that.canEqual(this) && this.hashCode() == that.hashCode()
    case _ => false
  }

  override def hashCode(): Int = event1.hashCode() * 7 + event2.hashCode() * 13 + relationType.hashCode() * 7
}
