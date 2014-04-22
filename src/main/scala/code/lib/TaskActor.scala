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
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Properties
import java.io.FileInputStream


class TaskActor extends Actor {

  val HIVE_FOLDER = "/data/dwlogs/tmplog"
  val MYSQL_FOLDER = "/tmp/dw_tmp_file"

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
    logger.info("Processing task id: " + taskId)

    val task = Task.find(taskId) openOr null

    if (task == null) {
      logger.error("Task not found.")
      return
    }

    task.status(Task.STATUS_RUNNING).save

    try {
      execute(task.id.get, task.query.get)
    } catch {
      case e: Exception =>
        logger.error(e, "Fail to execute query.")
        task.status(Task.STATUS_ERROR).save
        return
    }

    task.status(Task.STATUS_OK).save
  }

  private def execute(taskId: Long, query: String) {

    val ptrnExport = "(?i)EXPORT\\s+HIVE\\s+(\\w+)\\.(\\w+)\\s+TO\\s+MYSQL\\s+(\\w+)\\.(\\w+)(\\s+PARTITION\\s+(\\w+))?".r
    val buffer = MutableList[String]()

    for (sql <- removeComments(query).split(";")) {

      ptrnExport.findFirstMatchIn(sql) match {

        case Some(matcher) =>
          if (buffer.nonEmpty) {
            executeHive(taskId, buffer.mkString(";\n"))
            buffer.clear
          }
          exportHiveToMysql(taskId, matcher.group(1), matcher.group(2),
              matcher.group(3), matcher.group(4), matcher.group(6))

        case None => if (sql.trim.nonEmpty) buffer += sql
      }
    }

    if (buffer.nonEmpty) {
      executeHive(taskId, buffer.mkString(";\n"))
      buffer.clear
    }

  }

  private def removeComments(query: String) = {
    "(?s)/\\*.*?\\*/".r.replaceAllIn(query, "")
  }

  private def exportHiveToMysql(taskId: Long, hiveDatabase: String, hiveTable: String,
      mysqlDatabase: String, mysqlTable: String, partition: String) {

    // create mysql table
    val tableExists = DB.runQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_scheme = ? AND table_name = ?",
        List(mysqlDatabase, mysqlTable))._2(0)(0).toInt > 0

    if (!tableExists) {

      logger.info("Generating CREATE TABLE statement.")
      val createSql = new StringBuilder
      createSql ++= s"CREATE TABLE IF NOT EXISTS ${mysqlDatabase}.${mysqlTable} (\n"

      val hiveColumns = Seq("hive", "-e", s"USE ${hiveDatabase}; DESC ${hiveTable};").!!
      createSql ++= hiveColumns.split("\\n").map((line) => {
        val columnInfo = line.trim.split("\\s+")
        if (columnInfo.length > 1) {
          val columnType = columnInfo(1).toLowerCase match {
            case s if s.contains("bigint") => "BIGINT"
            case s if s.contains("int") => "INT"
            case s if s.contains("float") => "FLOAT"
            case s if s.contains("double") => "DOUBLE"
            case _ => "VARCHAR(255)"
          }
          s"  ${columnInfo(0)} ${columnType}"
        } else ""
      }).filter(!_.isEmpty).mkString(",\n")

      createSql ++= "\n)"

      runUpdate(createSql.toString)
    }

    // extract from hive
    val hiveSqlFile = s"${HIVE_FOLDER}/hive_server_task_${taskId}.sql"
    val dataFileName = "hive_server_task_${taskId}.txt"
    val hiveDataFile = s"${HIVE_FOLDER}/${dataFileName}"
    val mysqlDataFile = s"${MYSQL_FOLDER}/${dataFileName}"

    val fw = new FileWriter(hiveSqlFile)
    fw.write("SELECT * FROM ${hiveDatabase}.${hiveTable}")
    if (partition != null) {
      fw.write(" WHERE ${partition} = '${getDealDate}'")
    }
    fw.close

    runCmd(Seq("/home/hadoop/dwetl/exportHiveTableETLCustom.sh", hiveSqlFile, hiveDataFile))

    // rsync
    runCmd(Seq("rsync", "-vW", hiveDataFile, s"${getMysqlIp}::dw_tmp_file/${dataFileName}"))

    // load into mysql
    if (partition != null) {
      runUpdate("DELETE FROM ${mysqlDatabase}.${mysqlTable} WHERE ${partition} = '${getDealDate}'")
    } else {
      runUpdate("TRUNCATE TABLE ${mysqlDatabase}.${mysqlTable}")
    }
    runUpdate("LOAD DATA INFILE '${mysqlDataFile}' INTO TABLE ${mysqlDatabase}.${mysqlTable}")

  }

  private def executeHive(taskId: Long, sql: String) {
    val hiveSqlFile = s"${HIVE_FOLDER}/hive_server_task_${taskId}.sql"

    val fw = new FileWriter(hiveSqlFile)
    fw.write(sql)
    fw.close

    logger.info(sql)
    Seq("hive", "-f", hiveSqlFile).!
  }

  private def getDealDate = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
  }

  private def runCmd(cmd: Seq[String]): Int = {
    logger.info(cmd.mkString(" "))
    return cmd.!
  }

  private def runUpdate(sql: String): Int = {
    logger.info(sql)
    return DB.runUpdate(sql, Nil)
  }

  private def getMysqlIp(): String = {
    val userHome = System.getProperty("user.home")
    val input = new FileInputStream(s"${userHome}/dwetl/server_config/offline_dw-master.properties")
    val props = new Properties
    props.load(input)
    props.getProperty("remote.ip")
  }

}
