package code
package lib

import net.liftweb.common.Loggable
import net.liftweb.util.Props
import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import java.sql.{DriverManager, Connection, ResultSet}
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import code.util.CommandUtil

case class Column(val name: String, val dataType: String)

object TableRest extends RestHelper with Loggable {

  serve("api" / "table" prefix {

    case "list" :: database :: Nil JsonGet _ => {

      val conn = getConnection

      try {

        val result = query(conn, s"USE ${database}; SHOW TABLES")

        val tableList = new Iterator[String] {
          def hasNext = result.next
          def next = result.getString(1)
        } toList

        tableList: JValue

      } finally {
        conn.close
      }

    }

    case "desc" :: database :: table :: Nil JsonGet _ => {

      val sizeCommandFuture = Future {
        CommandUtil.run(Seq("hadoop", "fs", "-dus", s"/user/hive/warehouse/${database}.db/${table}"))
      }

      val sizeFuture = sizeCommandFuture map { result =>
        if (result.code == 0) {
          val lines = result.stdout.split("\n")
          val ptrn = "[0-9]+$".r
          lines.map(ptrn.findFirstIn _).filter(_.nonEmpty).headOption match {
            case Some(o) => o.get.toLong
            case None => 0
          }
        } else  0
      }

      val conn = getConnection

      try {

        // fetch columns
        case class HiveColumn(val name: String, val dataType: String, val comment: String)
        val columnResult = query(conn, s"USE $database; DESC FORMATTED $table")
        val columnLines = new Iterator[HiveColumn] {
          def hasNext = columnResult.next
          def next = HiveColumn(optString(columnResult, 1),
                                optString(columnResult, 2),
                                optString(columnResult, 3))
        }

        val fieldColumns = columnLines.drop(2).takeWhile(_.name.nonEmpty) map { line =>
          ("name" -> line.name) ~ ("type" -> line.dataType) ~ ("comment" -> line.comment)
        } toList

        val partitionColumns = {
          val nextColumnName = if (columnLines.hasNext) columnLines.next.name else null
          if (nextColumnName == "# Partition Information") {
            columnLines.drop(2).takeWhile(_.name.nonEmpty) map { line =>
              ("name" -> line.name) ~ ("type" -> line.dataType) ~ ("comment" -> line.comment) ~ ("partition" -> true)
            }
          } else Nil
        } toList

        val columns = fieldColumns ::: partitionColumns

        // fetch sample data
        val rowResult = query(conn, s"SET hive.mapred.mode = nonstrict; SELECT * FROM ${database}.${table} LIMIT 100")
        val rows = new Iterator[List[String]] {
            def hasNext = rowResult.next
            def next = {
              for (i <- 1 to columns.length) yield optString(rowResult, i)
            } toList
        }

        ("columns" -> columns) ~ ("rows" -> rows.toList) ~ ("size" -> Await.result(sizeFuture, 30 seconds))

      } finally {
        conn.close
      }

    }

  })

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

    stmt.setFetchSize(1000)
    stmt.executeQuery(sqls.last)

  }

  private def getMetaData(rs: ResultSet) = {
    val meta = rs.getMetaData
    val columns = for (i <- 1 to meta.getColumnCount) yield {
      Column(meta.getColumnLabel(i), meta.getColumnTypeName(i))
    }
    columns.toList
  }

  private def optString(rs: ResultSet, i: Int) = Option(rs.getString(i)) match {
    case Some(s) => s.trim
    case None => "NULL"
  }

}
