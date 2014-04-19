package code
package lib

import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonAST.JInt
import net.liftweb.json.JsonAST.JValue
import java.util.Date
import net.liftweb.common.Box
import net.liftweb.json.JsonAST.JString
import akka.actor.ActorRef

object TaskRest extends RestHelper {

  val taskActor = DependencyFactory.inject[ActorRef]

  serve("api" / "task" prefix {

    case "submit" :: Nil JsonGet _ => {
      taskActor.map(_ ! System.currentTimeMillis)
      JString("ok")
    }

  })

}
