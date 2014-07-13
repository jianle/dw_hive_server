package code.lib

import akka.actor.{Actor, Status}
import code.model.Task
import net.liftweb.common.{Loggable, Full, Empty}
import net.liftweb.mapper._
import code.util.{CommandUtil, HiveUtil}
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import scala.io.Source

object TaskActor {

  implicit val formats = DefaultFormats

  def createTask(json: JValue, status: Int): Task = {

    val query = (json \ "query").extractOrElse[String]("").trim
    val prefix = (json \ "prefix").extractOrElse[String]("").trim

    if (query.isEmpty) {
      throw new Exception("Query cannot be empty.")
    }

    Task.create
        .query(query)
        .prefix(prefix)
        .status(status)
        .saveMe()
  }

  def getErrorMessage(taskId: Long): String = {

    var errorMessage = ""

    try {
      val source = Source.fromFile(HiveUtil.errorFile(taskId))
      errorMessage = source.getLines.mkString("\n")
      source.close
    } catch {
      case e: Exception => errorMessage = "Unable to get error message."
    }

    errorMessage
  }

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
    case data: String => processJson(data)
    case _ => logger.debug("Unkown message.")
  }

  private def processJson(data: String): Unit = {

    try {

      val json = parse(data)
      (json \ "action").extract[String] match {
        case "enqueue" =>
          val task = createTask(json, Task.STATUS_PENDING)
          val res = ("status" -> "ok") ~ ("id" -> task.id.get)
          sender ! compact(render(res))
          logger.info("Enqueued task id " + task.id.get)

        case "execute" =>
          val taskId = (json \ "id").extract[Long]
          Task.find(taskId) match {
            case Full(task) =>
              if (task.status.get == Task.STATUS_PENDING) {
                process(taskId)
              } else {
                throw new Exception("Task is not pending.")
              }
            case _ => throw new Exception("Task not found.")
          }

          val task = Task.find(taskId).openOrThrowException("Task not found.")

          val res = task.status.get match {
            case Task.STATUS_OK => ("status", "ok") ~ ("taskStatus", "ok")
            case Task.STATUS_ERROR => ("status", "ok") ~ ("taskStatus", "error") ~ ("taskErrorMessage", getErrorMessage(task.id.get))
            case _ => throw new Exception("Task is not finished.")
          }

          sender ! compact(render(res))
      }

    } catch {
      case e: Exception =>
        logger.error("Fail to process actor request.", e)
        sender ! compact(render(("status" -> "error") ~ ("msg" -> e.toString)))
    }
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
