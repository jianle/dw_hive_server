package code
package lib

import akka.event.Logging
import akka.actor.Actor
import code.model.Task
import net.liftweb.common.Full
import scala.sys.process._
import net.liftweb.mapper._
import scala.collection.mutable.MutableList
import java.io.FileWriter
import java.io.File
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Properties
import java.io.FileInputStream
import scala.io.Source
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import net.liftweb.util.Props

object TaskActor {

  val HIVE_FOLDER = "/data/dwlogs/tmplog"
  val MYSQL_FOLDER = "/tmp/dw_tmp_file"
  val MAX_RESULT = 1000000

  def outputFile(taskId: Long) = {
    s"${HIVE_FOLDER}/hive_server_task_${taskId}.out"
  }

  def errorFile(taskId: Long) = {
    s"${HIVE_FOLDER}/hive_server_task_${taskId}.err"
  }

}

class TaskActor extends Actor {

  import TaskActor._

  val logger = Logging(context.system, this)

  override def preStart = {
    logger.info("taskActor started")
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

    try {
      execute(task)
      task.status(Task.STATUS_OK).save
    } catch {
      case e: Exception =>
        logger.error(e, "Fail to execute query.")
        task.status(Task.STATUS_ERROR).save

        val fw = new FileWriter(errorFile(task.id.get), true)
        fw.write(e.toString)
        fw.close

    } finally {
      logger.info(s"Task id ${taskId} finished.")
    }
  }

  private def execute(task: Task) {

    val ptrnH2m = "(?i)EXPORT\\s+HIVE\\s+(\\w+)\\.(\\w+)\\s+TO\\s+MYSQL\\s+(\\w+)\\.(\\w+)(\\s+PARTITION\\s+(\\w+))?".r
    val ptrnM2h = "(?i)EXPORT\\s+MYSQL\\s+(\\w+)\\.(\\w+)\\s+TO\\s+HIVE\\s+(\\w+)\\.(\\w+)".r

    val optH2m = ptrnH2m.findFirstMatchIn(task.query.get)
    val optM2h = ptrnM2h.findFirstMatchIn(task.query.get)

    if (optH2m.nonEmpty) {
      val matcher = optH2m.get
      exportHiveToMysql(taskId = task.id.get,
                        hiveDatabase = matcher.group(1),
                        hiveTable = matcher.group(2),
                        mysqlDatabase = matcher.group(3),
                        mysqlTable = matcher.group(4),
                        partition = matcher.group(6))
    } else if (optM2h.nonEmpty) {
      val matcher = optM2h.get
      exportMysqlToHive(taskId = task.id.get,
                        mysqlDatabase = matcher.group(1),
                        mysqlTable = matcher.group(2),
                        hiveDatabase = matcher.group(3),
                        hiveTable = matcher.group(4))
    } else {
      executeHive(task.id.get, task.query.get, task.prefix.get)
    }

  }

  private def exportHiveToMysql(taskId: Long, hiveDatabase: String, hiveTable: String,
      mysqlDatabase: String, mysqlTable: String, partition: String) {

    // create mysql table
    val tableExists = DB.runQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        List(mysqlDatabase, mysqlTable))._2(0)(0).toInt > 0

    if (!tableExists) {

      logger.info("Generating CREATE TABLE statement.")

      val result = CommandUtil.run(Seq("hive", "-e", s"USE ${hiveDatabase}; DESC ${hiveTable};"))

      if (result.code != 0) {
        throw new Exception("Hive table does not exist.")
      }

      val createSql = new StringBuilder(s"CREATE TABLE ${mysqlDatabase}.${mysqlTable} (\n")

      createSql ++= result.stdout.split("\n").map(_.trim).takeWhile(_.nonEmpty).map(line => {
        val columnInfo = line.split("\\s+")
        val columnType = columnInfo(1).toLowerCase match {
          case s if s.contains("bigint") => "BIGINT"
          case s if s.contains("int") => "INT"
          case s if s.contains("float") => "FLOAT"
          case s if s.contains("double") => "DOUBLE"
          case _ => "VARCHAR(255)"
        }
        s"  `${columnInfo(0)}` ${columnType}"
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
    fw.write(s" LIMIT $MAX_RESULT");
    fw.close

    runCmd(taskId, Seq("/home/hadoop/dwetl/exportHiveTableETLCustom.sh", hiveSqlFile, hiveDataFile))

    // rsync
    runCmd(taskId, Seq("rsync", "-vW", hiveDataFile, s"${getMysqlIp}::dw_tmp_file/${dataFileName}"))

    // load into mysql
    if (partition != null) {
      executeMysql(s"DELETE FROM ${mysqlDatabase}.${mysqlTable} WHERE ${partition} = '${getDealDate}'")
    } else {
      executeMysql(s"TRUNCATE TABLE ${mysqlDatabase}.${mysqlTable}")
    }
    executeMysql(s"LOAD DATA INFILE '${mysqlDataFile}' INTO TABLE ${mysqlDatabase}.${mysqlTable}")

  }

  private def exportMysqlToHive(taskId: Long, mysqlDatabase: String, mysqlTable: String, hiveDatabase: String, hiveTable: String) {

    // create hive table
    val tableExists = {
      val result = CommandUtil.run(Seq("hive", "-e", s"USE $hiveDatabase; SHOW TABLES LIKE '$hiveTable'"))
      result.code == 0 && result.stdout.trim == hiveTable
    }

    if (!tableExists) {

      logger.info("Generating CREATE TABLE statement.")

      val columns = DB.runQuery("SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
          List(mysqlDatabase, mysqlTable))._2

      if (columns.isEmpty) {
        throw new Exception("MySQL table does not exist.")
      }

      val createSql = new StringBuilder(s"CREATE TABLE ${hiveDatabase}.${hiveTable} (\n")

      createSql ++= columns.map(column => {
        val hiveType = column(1) match {
          case s if s.contains("bigint") => "BIGINT"
          case s if s.contains("int") => "INT"
          case s if s.contains("float") => "FLOAT"
          case s if s.contains("double") => "DOUBLE"
          case s if s.contains("decimal") => "DOUBLE"
          case _ => "STRING"
        }
        s"  `${column(0)}` $hiveType"
      }).mkString(",\n")

      createSql ++= "\n)\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';\n"

      executeHive(taskId, createSql.toString, "")
    }

    // extract from mysql
    val dataFileName = s"hive_server_task_${taskId}.txt"
    val hiveDataFile = s"${HIVE_FOLDER}/${dataFileName}"
    val mysqlDataFile = s"${MYSQL_FOLDER}/${dataFileName}"

    runCmd(taskId, Seq("ssh", s"dwadmin@${getMysqlIp}", "rm", "-f", mysqlDataFile))
    DB.use(DefaultConnectionIdentifier) { conn =>
      val sql = s"SELECT * FROM $mysqlDatabase.$mysqlTable LIMIT $MAX_RESULT INTO OUTFILE '$mysqlDataFile'"
      logger.info(s"MySQL - $sql")
      DB.exec(conn, sql) { rs => () }
    }

    // rsync
    runCmd(taskId, Seq("rsync", "-vW", s"${getMysqlIp}::dw_tmp_file/$dataFileName", hiveDataFile))

    // load into hive
    runCmd(taskId, Seq("hive", "-e", s"LOAD DATA LOCAL INPATH '$hiveDataFile' OVERWRITE INTO TABLE $hiveDatabase.$hiveTable"))

  }

  private def executeHive(taskId: Long, sql: String, prefix: String) {

    val hiveSqlFile = s"${HIVE_FOLDER}/hive_server_task_${taskId}.sql"

    val fw = new FileWriter(hiveSqlFile)
    fw.write(s"SET mapred.job.name = HS$taskId $prefix ${abridgeSql(sql)};\n")
    fw.write(sql)
    fw.close

    logger.info(s"Hive - ${sql}")
    runCmd(taskId, Seq("hive", "-f", hiveSqlFile), true)
  }

  private def getDealDate = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
  }

  private def runCmd(taskId: Long, cmd: Seq[String], mapred: Boolean = false) {

    logger.info(cmd.mkString(" "))

    var outputCounter = 0
    val outputWriter = new FileWriter(outputFile(taskId), true)
    val outputLogger = (line: String) => {
      if (outputCounter < MAX_RESULT) {
        outputWriter.write(line)
        outputWriter.write("\n")
        outputCounter += 1
      }
    }

    val errorWriter = new FileWriter(errorFile(taskId), true)
    val errorLogger = (line: String) => {
      errorWriter.write(line)
      errorWriter.write("\n")
    }

    val proc = cmd.run(ProcessLogger(outputLogger, errorLogger))

    val code = future {
      proc.exitValue
    }

    try {

        while (!Thread.interrupted) {

          // check task status
          val taskStatus = Task.findAllFields(Seq(Task.status), By(Task.id, taskId)).head.status.get
          if (taskStatus == Task.STATUS_INTERRUPTED) {
            proc.destroy
            if (mapred) {
              cleanupMapred(taskId)
            }
            throw new Exception("Task is interrupted.")
          }

          // poll result
          try {
            if (Await.result(code, 3 seconds) != 0) {
              throw new Exception("Command returns non-zero value.")
            }
            return
          } catch {
            case _: TimeoutException =>
          }

        }

    } finally {
      outputWriter.close
      errorWriter.close
    }
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

  private def cleanupMapred(taskId: Long) {

      import dispatch._

      val jt = Props.get("hadoop.jobtracker").openOrThrowException("hadoop.jobtracker not found")
      val res = Http(url(s"http://$jt/jobtracker.jsp") OK as.String).map(result => {

        val lines = result.split("\n")
            .dropWhile(!_.contains("<h2 id=\"running_jobs\">"))
            .takeWhile(!_.contains("<hr>"))

        val ptrn = ">(job_[0-9]+_[0-9]+)</a>.*?<td id=\"name_[0-9]+\">HS([0-9]+)".r
        lines.foreach(line => {
          ptrn.findFirstMatchIn(line) match {
            case Some(m) if m.group(2).toLong == taskId =>
              logger.info("Kill hadoop job: " + m.group(1))
              Seq("hadoop", "job", "-kill", m.group(1)) !
            case _ =>
          }
        })
      })
      res()

  }

  private def abridgeSql(sql: String): String = {

    var result = sql;

    // remove comments
    result = "(?s)/\\*.*?\\*/".r.replaceAllIn(result, "")
    result = "(?m)--.*$".r.replaceAllIn(result, "")

    // replace new line
    result = "[\\r\\n]+".r.replaceAllIn(result, " ")

    // remove buffer statements
    val ptrnBuffer = "(?i)^(SET|ADD\\s+JAR|CREATE\\s+TEMPORARY\\s+FUNCTION|USE)\\s+".r
    result = result.split(";").map(_.trim).filter(_.nonEmpty).filter(ptrnBuffer.findFirstIn(_).isEmpty).mkString("\\; ")

    result
  }

}
