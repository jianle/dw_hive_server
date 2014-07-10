package code.util

import java.io.FileInputStream
import java.util.Properties
import net.liftweb.common.Loggable
import scala.concurrent.{Future, Await, TimeoutException}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.sql.{DriverManager, Connection, Statement, ResultSet}

object MysqlUtil extends Loggable {

  private def getMysqlInfo(): Properties = {
    val userHome = System.getProperty("user.home")
    val input = new FileInputStream(s"${userHome}/dwetl/server_config/offline_dw-master.properties")
    val props = new Properties
    props.load(input)
    props
  }

  def getMysqlIp(): String = {
    getMysqlInfo.getProperty("remote.ip")
  }

  def getConnection(): Connection = {
    val info = getMysqlInfo
    DriverManager.getConnection(info.getProperty("remote.url"),
                                info.getProperty("remote.username"),
                                info.getProperty("remote.password"))
  }

  def runUpdate(conn: Connection, sql: String): Int = {
    logger.info(s"MySQL - $sql")
    val stmt = conn.createStatement
    stmt.executeUpdate(sql)
  }

  def runUpdate(conn: Connection, sql: String, isInterrupted: () => Boolean): Int = {

    logger.info(s"MySQL - $sql")

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

  def tableExists(conn: Connection, database: String, table: String): Boolean = {
    val stmt = conn.prepareStatement("SELECT COUNT(*) FROM information_schema.tables" +
                                     " WHERE table_schema = ? AND table_name = ?")
    stmt.setString(1, database)
    stmt.setString(2, table)
    val rs = stmt.executeQuery
    if (rs.next) rs.getInt(1) > 0 else false
  }

  def getColumns(conn: Connection, database: String, table: String): List[Column] = {
    val stmt = conn.prepareStatement("SELECT column_name, column_type FROM information_schema.columns" +
                                     " WHERE table_schema = ? AND table_name = ?")
    stmt.setString(1, database)
    stmt.setString(2, table)
    val rs = stmt.executeQuery
    new Iterator[Column] {
      def hasNext = rs.next
      def next = Column(rs.getString(1), rs.getString(2))
    } toList
  }

  def export(conn: Connection, database: String, table: String, filename: String, limit: Int): Unit = {
    val sql = s"SELECT * FROM $database.$table LIMIT $limit INTO OUTFILE '$filename'"
    logger.info(s"MySQL - $sql")

    val stmt = conn.createStatement
    stmt.executeUpdate(sql)
  }

}
