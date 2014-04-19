package code
package lib

import akka.event.Logging
import akka.actor.Actor


class TaskActor extends Actor {
  
  val log = Logging(context.system, this)
  
  override def preStart = {
    log.info("taskActor started")
    // TODO read unfinished tasks
  }
  
  override def postStop = {
    log.info("taskActor stopped")
  }
  
  def receive = {
    case ts: Long => log.info(ts.toString)
  }

}

object TaskActor {
  
}