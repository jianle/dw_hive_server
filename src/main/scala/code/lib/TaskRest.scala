package code
package lib

import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import code.model.Task
import net.liftweb.common.Loggable
import net.liftweb.http.S
import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonDSL._
import net.liftweb.common.Full

object TaskRest extends RestHelper with Loggable {

  val taskActor = DependencyFactory.inject[ActorRef]

  serve("api" / "task" prefix {

    case "submit" :: Nil JsonPost json -> _ => {

      (json \ "query").extractOpt[String] match {

        case Some(query) =>
          val task = Task.create
            .query(query)
            .status(Task.STATUS_NEW)
            .saveMe()

          taskActor.map(_ ! task.id.get)
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
        ("status", "ok") ~ ("taskStatus", taskStatus)
      }

    }

  })

}
