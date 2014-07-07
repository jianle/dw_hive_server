package code.lib

import org.apache.log4j.Logger
import scala.sys.process._

case class CommandResult(val code: Int, val stdout: String, val stderr: String)

object CommandUtil {

  private val logger = Logger.getLogger(CommandUtil.getClass)

  def run(cmd: Seq[String]): CommandResult = {

    logger.info(cmd.mkString(" "))

    val stdout = new StringBuilder
    val stderr = new StringBuilder

    val processLogger = ProcessLogger(line => {
      stdout ++= line
      stdout ++= "\n"
    }, line => {
      stderr ++= line
      stderr ++= "\n"
    })

    CommandResult(cmd ! processLogger, stdout.toString, stderr.toString)
  }

}
