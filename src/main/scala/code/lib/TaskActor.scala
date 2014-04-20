package code
package lib

import akka.event.Logging
import akka.actor.Actor
import code.model.Task
import net.liftweb.common.Full
import scala.sys.process._


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

    val task = Task.find(taskId) openOr null

    if (task == null) {
      log.error("Task not found.")
      return
    }

    task.status(Task.STATUS_RUNNING).save

    val cmd = Seq("ls", ".")
    log.info(cmd.mkString(" "))
    log.info(cmd.!!)

    task.status(Task.STATUS_OK).save
  }

}
