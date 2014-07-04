package code
package lib

import net.liftweb.common.Loggable
import net.liftweb.util.Props
import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import scala.sys.process._
import java.sql.{DriverManager, Connection, ResultSet}

case class Column(val name: String, val dataType: String)

object TableRest extends RestHelper with Loggable {

  serve("api" / "table" prefix {

    case "list" :: database :: Nil JsonGet _ => {
      val (status, stdout, stderr) = run(Seq("hive", "-e", s"USE ${database}; SHOW TABLES;"))

      val result = if (status == 0) {
        stdout.split("\n").toList
      } else {
        Nil
      }

      result: JValue
    }

    case "desc" :: database :: table :: Nil JsonGet _ => {

      import scala.concurrent._
      import scala.concurrent.duration._
      import ExecutionContext.Implicits._

      val hiveFuture = future {
        run(Seq("hive", "-e", s"USE ${database}; DESC FORMATTED ${table}; SET hive.cli.print.header = true; SELECT * FROM ${table} LIMIT 100"))
      }

      val sizeFuture = future {
        run(Seq("hadoop", "fs", "-du", "-s", s"/user/hive/warehouse/${database}.db/${table}"))
      }

      val hiveResult = Await.result(hiveFuture, 30 seconds)
      val sizeResult = Await.result(sizeFuture, 30 seconds)

      val columns = if (hiveResult._1 == 0) {
        val lines = hiveResult._2.split("\n").iterator.map(_.trim)

        val columnPattern = "([^\\s]+)\\s+([^\\s]+)\\s+(.*)".r
        val fieldColumns = lines.drop(2).takeWhile(_.nonEmpty).map(line => {
          columnPattern.findPrefixMatchOf(line) match {
            case Some(m) => ("name" -> m.group(1)) ~ ("type" -> m.group(2)) ~ ("comment" -> m.group(3))
            case None => JObject(Nil)
          }
        }).filter(_.obj.nonEmpty).toList

        val partitionColumns = if (lines.next == "# Partition Information") {
          lines.drop(2).takeWhile(_.nonEmpty).map(line => {
            columnPattern.findPrefixMatchOf(line) match {
              case Some(m) => ("name" -> m.group(1)) ~ ("type" -> m.group(2)) ~ ("comment" -> m.group(3)) ~ ("partition" -> true)
              case None => JObject(Nil)
            }
          }).filter(_.obj.nonEmpty).toList
        } else Nil

        fieldColumns ::: partitionColumns
      } else Nil

      val rows = if (columns.nonEmpty) {
        val columnRow = columns.map(column => (column \ "name").extract[String]).mkString("\t")
        val lines = hiveResult._2.split("\n").iterator.map(_.trim)
        lines.dropWhile(_ != columnRow).drop(1).map(line => {
          line.split("\t").take(columns.length).padTo(columns.length, "").toList
        }).toList
      } else Nil

      val size = if (sizeResult._1 == 0) {

        val lines = sizeResult._2.split("\n")
        val ptrn = "[0-9]+".r

        lines.map(line => {
          ptrn.findPrefixOf(line) match {
            case Some(s) => Some(s.toLong)
            case None => None
          }
        }).filter(_.nonEmpty).headOption match {
          case Some(o) => o.getOrElse(0L)
          case None => 0
        }

      } else 0

      ("columns" -> columns) ~ ("rows" -> rows) ~ ("size" -> size)
    }

    case "desc2" :: database :: table :: Nil JsonGet _ => {

      val conn = getConnection

      try {

        val rs = query(conn, "USE dw_stage; DESC zj_people")
        val columns = new Iterator[String] {
          def hasNext = rs.next
          def next = rs.getString(1)
        }

        ("columns" -> columns.toList): JValue

      } finally {
        conn.close
      }

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

  private def getConnection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val hiveserver2 = Props.get("hadoop.hiveserver2").openOrThrowException("hadoop.hiveserver2 not found")
    DriverManager.getConnection(s"jdbc:hive2://$hiveserver2", "hadoop", "")
  }

  private def query(conn: Connection, sql: String) = {

    val sqls = sql.split(";").map(_.trim).filter(_.nonEmpty)
    if (sqls.isEmpty) {
      throw new Exception("SQL cannot be empty.")
    }

    val stmt = conn.createStatement
    sqls.take(sqls.length - 1).foreach(stmt.execute _)
    stmt.executeQuery(sqls.last)

  }

  private def getMetaData(rs: ResultSet) = {
    val meta = rs.getMetaData
    val columns = for (i <- 1 to meta.getColumnCount) yield {
      Column(meta.getColumnLabel(i), meta.getColumnTypeName(i))
    }
    columns.toList
  }

}
