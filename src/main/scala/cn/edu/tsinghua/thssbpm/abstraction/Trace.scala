package cn.edu.tsinghua.thssbpm.abstraction

class Trace(_eventList:List[Event]) {
  val eventList:List[Event] = _eventList

  override def toString: String =
    eventList.toString()

}
