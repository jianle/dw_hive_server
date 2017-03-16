package code.lib

import akka.actor.ActorSystem
import akka.actor.{Props => akkaProps}
import net.liftweb.http.Factory
import net.liftweb.http.LiftRules
import net.liftweb.http.LiftRulesMocker.toLiftRules
import net.liftweb.util.Vendor.valToVender
import net.liftweb.util.Helpers
import net.liftweb.util.{Props => liftProps}
import akka.routing.BalancingPool
import com.typesafe.config.{ConfigFactory, Config}

object DependencyFactory extends Factory {

  private val actorSystem = {

    val config = ConfigFactory.parseString(Seq(
      "akka.remote.netty.tcp.hostname = \"%s\"" format liftProps.get("akka.remote.netty.hostname", ""),
      "akka.remote.netty.tcp.port = %d" format liftProps.getInt("akka.remote.netty.port", 2552)
    ) mkString "\n").withFallback(ConfigFactory.load)

    ActorSystem("hiveServer", config)
  }

  implicit object taskActor extends FactoryMaker(makeTaskActor)
  implicit object time extends FactoryMaker(Helpers.now)

  private def makeTaskActor = {
    val props = akkaProps[TaskActor]
        .withRouter(BalancingPool(10))
        .withDispatcher("task-dispatcher")
    actorSystem.actorOf(props, "taskActor")
  }

  private def init() {
    LiftRules.unloadHooks.append(actorSystem.shutdown)
    List(taskActor, time)
  }

  init()
}
