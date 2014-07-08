package code.util

import net.liftweb.common.Loggable
import scala.sys.process._
import scala.concurrent.{Future, Await, TimeoutException}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case class CommandResult(val code: Int, val stdout: String, val stderr: String)

object CommandUtil extends Loggable {

  def run(cmd: Seq[String]): CommandResult = {

    logger.info(cmd.mkString(" "))

    val (processLogger, stdout, stderr) = getProcessLogger

    CommandResult(cmd ! processLogger, stdout.toString, stderr.toString)
  }

  def run(cmd: Seq[String], isInterrupted: () => Boolean): CommandResult = {

    logger.info(cmd.mkString(" "))

    val (processLogger, stdout, stderr) = getProcessLogger

    val proc = cmd run processLogger
    val codeFuture = Future {
      proc.exitValue
    }

    while (!isInterrupted()) {
      try {
        return CommandResult(Await.result(codeFuture, 1 second), stdout.toString, stderr.toString)
      } catch {
        case _: TimeoutException =>
      }
    }

    proc.destroy
    throw new Exception("Command is interrupted.")
  }

  private def getProcessLogger = {

    val stdout = new StringBuilder
    val stderr = new StringBuilder

    val processLogger = ProcessLogger(line => {
      stdout ++= line
      stdout ++= "\n"
    }, line => {
      stderr ++= line
      stderr ++= "\n"
    })

    (processLogger, stdout, stderr)
  }

}
