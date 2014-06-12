package code
package lib

import net.liftweb.common.Loggable
import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonDSL._
import scala.sys.process._
import net.liftweb.json.JsonAST._


object TableRest extends RestHelper with Loggable {

  serve("api" / "table" prefix {

    case "list" :: database JsonGet _ => {
      val (status, stdout, stderr) = run(Seq("hive", "-e", s"USE ${database(0)}; SHOW TABLES;"))

      val result = if (status == 0) {
        stdout.split("\n").toList
      } else {
        Nil
      }

      result: JValue
    }

    case "desc" :: database :: table JsonGet _ => {

      import scala.concurrent._
      import scala.concurrent.duration._
      import ExecutionContext.Implicits._

      val columnsFuture = future {
        val (status, stdout, stderr) = run(Seq("hive", "-e", s"USE ${database}; DESC FORMATTED ${table(0)}"))
        if (status == 0) stdout else stderr
      }

      val sizeFuture = future {
        val (status, stdout, stderr) = run(Seq("hadoop", "fs", "-du", s"/user/hive/warehouse/${database}.db/${table(0)}"))
        if (status == 0) stdout else stderr
      }

      val sampleFuture = future {
        val (status, stdout, stderr) = run(Seq("hive", "-e", s"SELECT * FROM ${database}.${table(0)} LIMIT 100"))
        if (status == 0) stdout else stderr
      }

      val result = Await.result(columnsFuture, 30 seconds) ::
          Await.result(sizeFuture, 30 seconds) ::
          Await.result(sampleFuture, 30 seconds) ::
          Nil

      result: JValue
    }

  })

  private def run(cmd: Seq[String]) = {

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

    (cmd ! processLogger, stdout.toString, stderr.toString)
  }

}
