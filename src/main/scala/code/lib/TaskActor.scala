package code.lib

import akka.actor.Actor
import code.model.Task
import net.liftweb.common.Loggable
import net.liftweb.mapper._
import code.util.{CommandUtil, HiveUtil}

object TaskActor {

}

class TaskActor extends Actor with Loggable {

  import TaskActor._

  override def preStart = {
    logger.info("taskActor started")
  }

  override def postStop = {
    logger.info("taskActor stopped")
  }

  def receive = {
    case taskId: Long => process(taskId)
    case _ => logger.debug("Unkown message.")
  }

  private def process(taskId: Long): Unit = {

    logger.info(s"Processing task id: ${taskId}")

    val task = Task.find(taskId) openOr null
    if (task == null) {
      logger.error("Task not found.")
      return
    }

    task.status(Task.STATUS_RUNNING).save

    try {
      HiveUtil.execute(task)
      task.status(Task.STATUS_OK).save
    } catch {
      case e: Exception =>
        logger.error("Fail to execute query.", e)
        task.status(Task.STATUS_ERROR).save
        HiveUtil.writeError(e.toString)(task.id.get)
    } finally {
      logger.info(s"Task id ${taskId} finished.")
    }
  }

}
