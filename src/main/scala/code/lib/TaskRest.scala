package code
package lib

import akka.actor.ActorRef
import code.model.Task
import net.liftweb.http.S
import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._

object TaskRest extends RestHelper {

  val taskActor = DependencyFactory.inject[ActorRef]

  serve("api" / "task" prefix {

    case "submit" :: Nil JsonGet _ => {

      val query = S.param("query") openOr ""

      if (query isEmpty) {
        ("status", "error") ~ ("msg", "Query cannot be empty.")
      } else {

        val task = Task.create
            .query(query)
            .status(Task.STATUS_NEW)
            .saveMe()

        val taskId = task.id.get.intValue
        taskActor.map(_ ! taskId)

        ("status", "ok") ~ ("id", taskId)
      }

    }

  })

}
