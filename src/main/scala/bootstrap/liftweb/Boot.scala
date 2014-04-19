package bootstrap.liftweb

import code.lib.TaskRest
import net.liftweb.db.DB1.db1ToDb
import net.liftweb.http.LiftRules
import net.liftweb.http.LiftRulesMocker.toLiftRules
import net.liftweb.http.S
import net.liftweb.mapper.DB
import net.liftweb.mapper.DefaultConnectionIdentifier
import net.liftweb.mapper.MapperRules
import net.liftweb.mapper.StandardDBVendor
import net.liftweb.util.Helpers
import net.liftweb.util.Props
import code.lib.TaskActor
import akka.actor.ActorSystem

class Boot {
  def boot {
    val vendor =
      new StandardDBVendor(
          Props.get("db.driver") openOr "com.mysql.jdbc.Driver",
          Props.get("db.url") openOr "jdbc:mysql://localhost",
          Props.get("db.user"),
          Props.get("db.password"))

    LiftRules.unloadHooks.append(vendor.closeAllConnections_! _)

    DB.defineConnectionManager(DefaultConnectionIdentifier, vendor)

    MapperRules.columnName = (_,name) => Helpers.snakify(name)

    // where to search snippet
    LiftRules.addToPackages("code")

    LiftRules.statelessDispatch.append(TaskRest)

    // Force the request to be UTF-8
    LiftRules.early.append(_.setCharacterEncoding("UTF-8"))

    // Make a transaction span the whole HTTP request
    S.addAround(DB.buildLoanWrapper)

    // Start daemon
    val actorSystem = ActorSystem("hiveServer")
    val taskActor = actorSystem.actorOf(akka.actor.Props[TaskActor], "taskActor")
    LiftRules.unloadHooks.append(() => actorSystem.shutdown)

  }
}
