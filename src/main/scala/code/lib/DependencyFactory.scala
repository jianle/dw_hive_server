package code
package lib

import akka.actor.ActorSystem
import akka.actor.Props
import net.liftweb.http.Factory
import net.liftweb.http.LiftRules
import net.liftweb.http.LiftRulesMocker.toLiftRules
import net.liftweb.util.Vendor.valToVender
import net.liftweb.util.Helpers

object DependencyFactory extends Factory {

  private val actorSystem = ActorSystem("hiveServer")
  implicit object taskActor extends FactoryMaker(actorSystem.actorOf(Props[TaskActor], "taskActor"))
  implicit object time extends FactoryMaker(Helpers.now)

  private def init() {
    LiftRules.unloadHooks.append(actorSystem.shutdown)
    List(taskActor, time)
  }

  init()
}
