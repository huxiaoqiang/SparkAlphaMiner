package cn.edu.tsinghua.thssbpm.abstraction

@SerialVersionUID(1000L)
class Event(n: String) extends Serializable {
  val name: String = n

  override def toString: String = name

  def canEqual(e: Any) = e.isInstanceOf[Event]

  override def equals(that: scala.Any): Boolean = that match {
    case that: Event => that.canEqual(this) && this.hashCode() == that.hashCode()
    case _ => false
  }

  //can not deal with duplication of name
  override def hashCode(): Int = name.hashCode
}
