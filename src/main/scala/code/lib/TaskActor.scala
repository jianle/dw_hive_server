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
    case taskId: Int => process(taskId)
    case _ => log.debug("Unkown message.")
  }

  private def process(taskId: Int) {
    log.info("Processing task id: " + taskId)
  }

}
