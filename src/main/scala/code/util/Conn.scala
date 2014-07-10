package code.util

import net.liftweb.common.Loggable
import java.sql.Connection

class Conn extends Loggable {

  var _mysql: Connection = null
  var _hive: Connection = null

  def mysql: Connection = {
    if (_mysql == null) _mysql = MysqlUtil.getConnection
    _mysql
  }

  def hive: Connection = {
    if (_hive == null) _hive = HiveUtil.getConnection
    _hive
  }

  implicit def enrichConnection(conn: Connection) = new {
    def closeQuietly(): Unit = {
      try {
        conn.close
      } catch {
        case e: Exception => logger.debug("Fail to close connection.", e)
      }
    }
  }

  def close(): Unit = {
    if (_mysql != null) _mysql.closeQuietly
    if (_hive != null) _hive.closeQuietly
  }

}
