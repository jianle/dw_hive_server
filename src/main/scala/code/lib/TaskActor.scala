package code
package lib

import akka.event.Logging
import akka.actor.Actor
import code.model.Task
import net.liftweb.common.Full
import scala.sys.process._
import net.liftweb.mapper.ByList
import scala.collection.mutable.MutableList
import net.liftweb.mapper.DB
import java.io.FileWriter
import java.io.File
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Properties
import java.io.FileInputStream


object TaskActor {
  val HIVE_FOLDER = "/data/dwlogs/tmplog/hs"
  val MYSQL_FOLDER = "/tmp/dw_tmp_file"
}

class TaskActor extends Actor {

  import TaskActor._

  val logger = Logging(context.system, this)

  override def preStart = {
    logger.info("taskActor started")

    // restore unfinished tasks
    val taskList = Task.findAll(
        ByList(Task.status, List(Task.STATUS_NEW, Task.STATUS_RUNNING)))

    taskList.map((task) => {
      self ! task.id.get
      logger.info("Restored task id " + task.id.get)
    })
  }

  override def postStop = {
    logger.info("taskActor stopped")
  }

  def receive = {
    case taskId: Long => process(taskId)
    case _ => logger.debug("Unkown message.")
  }

  private def process(taskId: Long) {
    logger.info(s"Processing task id: ${taskId}")

    val task = Task.find(taskId) openOr null

    if (task == null) {
      logger.error("Task not found.")
      return
    }

    task.status(Task.STATUS_RUNNING).save

    val errorFile = new File(s"${HIVE_FOLDER}/hive_server_task_${taskId}.err")
    try {
      execute(task.id.get, errorFile, task.query.get)
      task.status(Task.STATUS_OK).save
    } catch {
      case e: Exception =>
        logger.error(e, "Fail to execute query.")
        task.status(Task.STATUS_ERROR).save

        val fw = new FileWriter(errorFile, true)
        fw.write(e.toString)
        fw.close

    } finally {
      logger.info(s"Task id ${taskId} finished.")
    }
  }

  private def execute(taskId: Long, errorFile: File, query: String) {

    val ptrnExport = "(?i)EXPORT\\s+HIVE\\s+(\\w+)\\.(\\w+)\\s+TO\\s+MYSQL\\s+(\\w+)\\.(\\w+)(\\s+PARTITION\\s+(\\w+))?".r
    val buffer = MutableList[String]()

    for (sql <- removeComments(query).split(";")) {

      ptrnExport.findFirstMatchIn(sql) match {

        case Some(matcher) =>
          if (buffer.nonEmpty) {
            executeHive(taskId, errorFile, buffer.mkString(";"))
            buffer.clear
          }
          exportHiveToMysql(taskId, errorFile, matcher.group(1), matcher.group(2),
              matcher.group(3), matcher.group(4), matcher.group(6))

        case None => if (sql.trim.nonEmpty) buffer += sql
      }
    }

    if (buffer.nonEmpty) {
      executeHive(taskId, errorFile, buffer.mkString(";"))
      buffer.clear
    }

  }

  private def removeComments(query: String) = {
    "(?s)/\\*.*?\\*/".r.replaceAllIn(query, "")
  }

  private def exportHiveToMysql(taskId: Long, errorFile: File, hiveDatabase: String, hiveTable: String,
      mysqlDatabase: String, mysqlTable: String, partition: String) {

    // create mysql table
    val tableExists = DB.runQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        List(mysqlDatabase, mysqlTable))._2(0)(0).toInt > 0

    if (!tableExists) {

      logger.info("Generating CREATE TABLE statement.")
      val createSql = new StringBuilder
      createSql ++= s"CREATE TABLE ${mysqlDatabase}.${mysqlTable} (\n"

      val hiveColumns = runCmd(Seq("hive", "-e", s"USE ${hiveDatabase}; DESC ${hiveTable};"), errorFile, true)
      createSql ++= hiveColumns.split("\\n").takeWhile(_.trim.nonEmpty).map((line) => {
        val columnInfo = line.trim.split("\\s+")
        val columnType = columnInfo(1).toLowerCase match {
          case s if s.contains("bigint") => "BIGINT"
          case s if s.contains("int") => "INT"
          case s if s.contains("float") => "FLOAT"
          case s if s.contains("double") => "DOUBLE"
          case _ => "VARCHAR(255)"
        }
        s"  ${columnInfo(0)} ${columnType}"
      }).mkString(",\n")

      createSql ++= "\n)"

      executeMysql(createSql.toString)
    }

    // extract from hive
    val hiveSqlFile = s"${HIVE_FOLDER}/hive_server_task_${taskId}.sql"
    val dataFileName = s"hive_server_task_${taskId}.txt"
    val hiveDataFile = s"${HIVE_FOLDER}/${dataFileName}"
    val mysqlDataFile = s"${MYSQL_FOLDER}/${dataFileName}"

    val fw = new FileWriter(hiveSqlFile)
    fw.write(s"SELECT * FROM ${hiveDatabase}.${hiveTable}")
    if (partition != null) {
      fw.write(s" WHERE ${partition} = '${getDealDate}'")
    }
    fw.close

    runCmd(Seq("/home/hadoop/dwetl/exportHiveTableETLCustom.sh", hiveSqlFile, hiveDataFile), errorFile)

    // rsync
    runCmd(Seq("rsync", "-vW", hiveDataFile, s"${getMysqlIp}::dw_tmp_file/${dataFileName}"), errorFile)

    // load into mysql
    if (partition != null) {
      executeMysql(s"DELETE FROM ${mysqlDatabase}.${mysqlTable} WHERE ${partition} = '${getDealDate}'")
    } else {
      executeMysql(s"TRUNCATE TABLE ${mysqlDatabase}.${mysqlTable}")
    }
    executeMysql(s"LOAD DATA INFILE '${mysqlDataFile}' INTO TABLE ${mysqlDatabase}.${mysqlTable}")

  }

  private def executeHive(taskId: Long, errorFile: File, sql: String) {
    val hiveSqlFile = s"${HIVE_FOLDER}/hive_server_task_${taskId}.sql"

    val fw = new FileWriter(hiveSqlFile)
    fw.write(sql)
    fw.close

    logger.info(s"Hive - ${sql}")
    runCmd(Seq("hive", "-f", hiveSqlFile), errorFile)
  }

  private def getDealDate = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
  }

  private def runCmd(cmd: Seq[String], errorFile: File, output: Boolean = false): String = {
    logger.info(cmd.mkString(" "))

    val fw = new FileWriter(errorFile, true)
    val errLogger = (line: String) => {
      fw.write(line)
      fw.write('\n')
    }

    val sb = new StringBuilder
    val processLogger = if (output) {
      ProcessLogger((line) => {
        sb ++= line
        sb += '\n'
      }, errLogger)
    } else {
      ProcessLogger((line) => (), errLogger)
    }

    val returnValue = cmd ! processLogger

    fw.close

    if (returnValue != 0) {
      throw new Exception("Command return non-zero value.")
    }

    sb.toString
  }

  private def executeMysql(sql: String) {
    logger.info(s"MySQL - ${sql}")
    DB.runUpdate(sql, Nil)
  }

  private def getMysqlIp(): String = {
    val userHome = System.getProperty("user.home")
    val input = new FileInputStream(s"${userHome}/dwetl/server_config/offline_dw-master.properties")
    val props = new Properties
    props.load(input)
    props.getProperty("remote.ip")
  }

}
