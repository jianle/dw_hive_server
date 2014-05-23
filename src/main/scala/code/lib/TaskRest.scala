package code
package lib

import scala.io.Source
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import code.model.Task
import net.liftweb.common.Loggable
import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonDSL._
import net.liftweb.mapper.ByList

object TaskRest extends RestHelper with Loggable {

  val taskActor = DependencyFactory.inject[ActorRef]

  private def init() {

    // restore unfinished tasks
    val taskList = Task.findAll(
        ByList(Task.status, List(Task.STATUS_NEW, Task.STATUS_RUNNING)))

    taskList.map(task => {
      taskActor.map(_ ! task.id.get)
      logger.info("Restored task id " + task.id.get)
    })

  }

  init()

  serve("api" / "task" prefix {

    case "submit" :: Nil JsonPost json -> _ => {

      (json \ "query").extractOpt[String] match {

        case Some(query) =>
          val task = Task.create
            .query(query)
            .status(Task.STATUS_NEW)
            .saveMe()

          task.doPostCommit(() => taskActor.map(_ ! task.id.get))

          logger.info("Submitted task id " + task.id.get)

          ("status", "ok") ~ ("id", task.id.get)

        case None => ("status", "error") ~ ("msg", "Query cannot be empty.")
      }

    }

    case "status" :: taskId JsonGet _ => {

      val task = Task.find(taskId(0).toLong) openOr null

      if (task == null) {
        ("status", "error") ~ ("msg", "Task id not found.")
      } else {

        val taskStatus = task.status.get match {
          case Task.STATUS_NEW => "new"
          case Task.STATUS_RUNNING => "running"
          case Task.STATUS_OK => "ok"
          case Task.STATUS_ERROR => "error"
        }

        if (taskStatus.equals("error")) {

          var errorMessage = ""

          try {
            val source = Source.fromFile(s"${TaskActor.HIVE_FOLDER}/hive_server_task_${task.id.get}.err")
            errorMessage = source.getLines.mkString("\n")
            source.close
          } catch {
            case e: Exception => errorMessage = "Unable to get error message."
          }

          ("status", "ok") ~ ("taskStatus", taskStatus) ~ ("taskErrorMessage", errorMessage)
        } else {
          ("status", "ok") ~ ("taskStatus", taskStatus)
        }
      }

    }

  })

}
