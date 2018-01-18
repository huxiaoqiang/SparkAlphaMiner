package demo

import scala.xml._
object ReadLog {

  //Read xml log in local file system
  val log9 = "/home/ubuntu/9_add_start.xes"
  def main(args: Array[String]): Unit = {

    val logFile = XML.loadFile(log9)
    val traces = logFile \ "trace"

    val traceList = traces.
      map(
        x => x.\("event")
        .map(e => e.\("string").filter(_.\\("@key").toString() == "concept:name"))
        .map(_.\\("@value").toString())
      )
    println(traceList)
    // List(List(trace),List(trace))
  }
}

