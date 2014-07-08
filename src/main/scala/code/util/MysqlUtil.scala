package code.util

import java.io.FileInputStream
import java.util.Properties
import net.liftweb.common.Loggable
import net.liftweb.db.{DB, DefaultConnectionIdentifier}
import scala.concurrent.{Future, Await, TimeoutException}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object MysqlUtil extends Loggable {

  def getMysqlIp(): String = {
    val userHome = System.getProperty("user.home")
    val input = new FileInputStream(s"${userHome}/dwetl/server_config/offline_dw-master.properties")
    val props = new Properties
    props.load(input)
    props.getProperty("remote.ip")
  }

  def runUpdate(sql: String): Int = {
    logger.info(s"MySQL - $sql")
    DB.runUpdate(sql, Nil)
  }

  def runUpdate(sql: String, isInterrupted: () => Boolean): Int = {

    logger.info(s"MySQL - $sql")

    DB.use(DefaultConnectionIdentifier) { conn =>

      val stmt = conn.createStatement
      val stmtFuture = Future {
        stmt.executeUpdate(sql)
      }

      while (!isInterrupted()) {
        try {
          return Await.result(stmtFuture, 1 second)
        } catch {
          case _: TimeoutException =>
        }
      }

      stmt.cancel
      throw new Exception("Query is interrupted.")
    }
  }

  def tableExists(database: String, table: String): Boolean = {
    DB.runQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        List(database, table))._2(0)(0).toInt > 0
  }

  def getColumns(database: String, table: String): List[Column] = {
    val columns = DB.runQuery("SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
          List(database, table))._2
    columns map { column =>
      Column(column(0), column(1))
    }
  }

  def export(database: String, table: String, filename: String, limit: Int) = {
    DB.use(DefaultConnectionIdentifier) { conn =>
      val sql = s"SELECT * FROM $database.$table LIMIT $limit INTO OUTFILE '$filename'"
      logger.info(s"MySQL - $sql")
      DB.exec(conn, sql) { rs => () }
    }
  }

}
