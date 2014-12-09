package code.util

import net.liftweb.common.Loggable
import net.liftweb.util.Props
import java.io.{FileWriter, FileOutputStream}
import code.model.Task
import java.util.Calendar
import java.text.SimpleDateFormat
import java.sql.{DriverManager, Connection, ResultSet, Statement}
import scala.concurrent.{Future, Await, TimeoutException}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys.process._
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.apache.commons.lang.mutable.MutableLong

case class Column(val name: String, val dataType: String, val comment: String = "")

object HiveUtil extends Loggable {

  val HIVE_FOLDER = "/data/dwlogs/tmplog"
  val MYSQL_FOLDER = "/tmp/dw_tmp_file"
  val MAX_RESULT = 1000000
  val FETCH_SIZE = 1000

  def outputFile(implicit taskId: Long): String = {
    s"${HIVE_FOLDER}/hive_server_task_${taskId}.out"
  }

  def errorFile(implicit taskId: Long): String = {
    s"${HIVE_FOLDER}/hive_server_task_${taskId}.err"
  }

  def metaFile(implicit taskId: Long): String = {
    s"${HIVE_FOLDER}/hive_server_task_${taskId}.meta"
  }

  def writeError(content: String, append: Boolean = true)(implicit taskId: Long): Unit = {
    val fw = new FileWriter(errorFile(taskId), append)
    fw.write(content)
    fw.close
  }

  implicit val formats = DefaultFormats

  def execute(task: Task) {

    implicit val taskId = task.id.get
    implicit val conn = new Conn

    try {

      val ptrnH2m = "(?i)EXPORT\\s+HIVE\\s+(\\w+)\\.(\\w+)\\s+TO\\s+MYSQL\\s+(\\w+)\\.(\\w+)(\\s+PARTITION\\s+(\\w+))?".r
      val ptrnM2h = "(?i)EXPORT\\s+MYSQL\\s+(\\w+)\\.(\\w+)\\s+TO\\s+HIVE\\s+(\\w+)\\.(\\w+)".r

      val optH2m = ptrnH2m.findFirstMatchIn(task.query.get)
      val optM2h = ptrnM2h.findFirstMatchIn(task.query.get)

      if (optH2m.nonEmpty) {
        val matcher = optH2m.get
        exportHiveToMysql(hiveDatabase = matcher.group(1),
                          hiveTable = matcher.group(2),
                          mysqlDatabase = matcher.group(3),
                          mysqlTable = matcher.group(4),
                          partition = matcher.group(6))
      } else if (optM2h.nonEmpty) {
        val matcher = optM2h.get
        exportMysqlToHive(mysqlDatabase = matcher.group(1),
                          mysqlTable = matcher.group(2),
                          hiveDatabase = matcher.group(3),
                          hiveTable = matcher.group(4))
      } else {
        executeHive(task.query.get, task.prefix.get)
      }

    } finally {
      conn.close
    }

  }

  private def exportHiveToMysql(hiveDatabase: String, hiveTable: String,
      mysqlDatabase: String, mysqlTable: String,
      partition: String)(implicit taskId: Long, conn: Conn) {

    ensureMysqlTable(hiveDatabase, hiveTable, mysqlDatabase, mysqlTable)

    val isInterrupted = () => Task.isInterrupted(taskId)

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

    CommandUtil.run(Seq("/home/hadoop/dwetl/exportHiveTableETLCustom.sh", hiveSqlFile, hiveDataFile), isInterrupted)

    // rsync
    CommandUtil.run(Seq("rsync", "-vW", hiveDataFile, s"${MysqlUtil.getMysqlIp}::dw_tmp_file/${dataFileName}"), isInterrupted)

    // delete from mysql
    if (partition != null) {
      MysqlUtil.runUpdate(conn.mysql, s"DELETE FROM ${mysqlDatabase}.${mysqlTable} WHERE ${partition} = '${getDealDate}'", isInterrupted)
    } else {
      MysqlUtil.runUpdate(conn.mysql, s"TRUNCATE TABLE ${mysqlDatabase}.${mysqlTable}")
    }

    // re-order columns
    val columns = getColumns(conn.hive, hiveDatabase, hiveTable).map(_.name)
    val columnsOrdered = if (partition != null) {
      columns.filter(_ != partition) :+ partition
    } else {
      columns
    }
    val mysqlColumns = columnsOrdered.map(name => s"`${name}`").mkString(", ")

    // load into mysql
    MysqlUtil.runUpdate(conn.mysql, s"LOAD DATA INFILE '${mysqlDataFile}' INTO TABLE ${mysqlDatabase}.${mysqlTable} (${mysqlColumns})", isInterrupted)

    touchOutputFiles()
  }

  private def ensureMysqlTable(hiveDatabase: String, hiveTable: String,
      mysqlDatabase: String, mysqlTable: String)(implicit conn: Conn): Unit = {

    if (MysqlUtil.tableExists(conn.mysql, mysqlDatabase, mysqlTable)) {
      return
    }

    logger.info("Creating MySQL table...")

    val columns = getColumns(conn.hive, hiveDatabase, hiveTable)
    if (columns.isEmpty) {
      throw new Exception("Hive table does not exist.")
    }

    val createSql = new StringBuilder(s"CREATE TABLE ${mysqlDatabase}.${mysqlTable} (\n")

    val columnsMapped = columns map { column =>
      val columnType = column.dataType.toLowerCase match {
        case s if s.startsWith("bigint") => "BIGINT"
        case s if s.startsWith("int") => "INT"
        case s if s.startsWith("float") => "FLOAT"
        case s if s.startsWith("double") => "DOUBLE"
        case _ => "VARCHAR(255)"
      }
      s"  `${column.name}` ${columnType}"
    }
    createSql ++= columnsMapped mkString ",\n"

    createSql ++= "\n)"

    MysqlUtil.runUpdate(conn.mysql, createSql.toString)

  }

  private def getColumns(conn: Connection, database: String, table: String): List[Column] = {
    val rs = runQuery(conn, s"USE ${database}; DESC ${table}")._1
    fetchResult(rs) map { row =>
      Column(row(0), row(1), row(2))
    }
  }

  private def exportMysqlToHive(mysqlDatabase: String, mysqlTable: String,
      hiveDatabase: String, hiveTable: String)(implicit taskId: Long, conn: Conn) {

    ensureHiveTable(mysqlDatabase, mysqlTable, hiveDatabase, hiveTable)

    val isInterrupted = () => Task.isInterrupted(taskId)

    // extract from mysql
    val dataFileName = s"hive_server_task_${taskId}.txt"
    val hiveDataFile = s"${HIVE_FOLDER}/${dataFileName}"
    val mysqlDataFile = s"${MYSQL_FOLDER}/${dataFileName}"

    CommandUtil.run(Seq("ssh", s"dwadmin@${MysqlUtil.getMysqlIp}", "rm", "-f", mysqlDataFile))
    MysqlUtil.export(conn.mysql, mysqlDatabase, mysqlTable, mysqlDataFile, MAX_RESULT)

    // rsync
    CommandUtil.run(Seq("rsync", "-vW", s"${MysqlUtil.getMysqlIp}::dw_tmp_file/$dataFileName", hiveDataFile), isInterrupted)

    // load into hive
    CommandUtil.run(Seq("hive", "-e", s"LOAD DATA LOCAL INPATH '$hiveDataFile' OVERWRITE INTO TABLE $hiveDatabase.$hiveTable"), isInterrupted)

    touchOutputFiles()
  }

  private def touchOutputFiles()(implicit taskId: Long) = {
    new FileOutputStream(outputFile).close
    new FileOutputStream(metaFile).close
  }

  private def ensureHiveTable(mysqlDatabase: String, mysqlTable: String,
      hiveDatabase: String, hiveTable: String)(implicit conn: Conn): Unit = {

    val rs = runQuery(conn.hive, s"USE ${hiveDatabase}; SHOW TABLES LIKE '${hiveTable}'")._1
    if (rs.next) {
      return
    }

    logger.info("Creating Hive table...")

    val columns = MysqlUtil.getColumns(conn.mysql, mysqlDatabase, mysqlTable)
    if (columns.isEmpty) {
      throw new Exception("MySQL table does not exist.")
    }

    val createSql = new StringBuilder(s"CREATE TABLE ${hiveDatabase}.${hiveTable} (\n")

    val columnsMapped = columns map { column =>
      val hiveType = column.dataType match {
        case s if s.startsWith("bigint") => "BIGINT"
        case s if s.matches("^(tiny|small|medium)?int.*") => "INT"
        case s if s.startsWith("float") => "FLOAT"
        case s if s.startsWith("double") => "DOUBLE"
        case s if s.startsWith("decimal") => "DOUBLE"
       case _ => "STRING"
      }
      s"  `${column.name}` $hiveType"
    }
    createSql ++= columnsMapped mkString ",\n"

    createSql ++= "\n)\nROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';\n"

    runQuery(conn.hive, createSql.toString)
  }

  private def executeHive(sql: String, prefix: String)(implicit taskId: Long, conn: Conn): Unit = {

    val sqlWithoutComments = removeComments(sql)
    val sqlAbridged = abridgeSql(sqlWithoutComments)
    val sqlWithPrefix = s"SET mapred.job.name = HS$taskId $prefix ${sqlAbridged};\n" + sqlWithoutComments

    val fw = new FileWriter(outputFile, true)
    val meta = new FileWriter(metaFile, true)

    try {

      val (rs, uc) = runQuery(sqlWithPrefix, () => Task.isInterrupted(taskId))

      if (rs != null) {

        val columns = getColumns(rs)
        fw.write(columns.map(_.name).mkString("\t"))
        fw.write("\n")

        val rows = new Iterator[String] {
          def hasNext = rs.next
          def next = {
            for (i <- 1 to columns.length) yield optString(rs, i, "NULL")
          } mkString "\t"
        }

        val numRows = new MutableLong
        rows.take(MAX_RESULT) foreach { line =>
          fw.write(line)
          fw.write("\n")
          numRows.increment
        }

        // meta
        val metaColumns = columns map { column =>
          ("name" -> column.name) ~ ("dataType" -> column.dataType)
        }
        val metaContent = compact(render(("columns" -> metaColumns) ~ ("rows" -> numRows.longValue)))
        meta.write(metaContent)

      }

    } finally {
      fw.close
      meta.close
    }
  }

  private def getDealDate = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
  }

  private def cleanupMapred(implicit taskId: Long): Unit = {

    import org.apache.hadoop.conf._
    import org.apache.hadoop.mapred._

    val jt = Props.get("hadoop.jobtracker").openOrThrowException("hadoop.jobtracker not found")
    val conf = new Configuration
    conf.set("mapred.job.tracker", jt)
    val client = new JobClient(conf)

    try {

      val ptrn = "HS([0-9]+)".r
      for (jobStatus <- client.jobsToComplete) {

        val job = client.getJob(jobStatus.getJobID)
        val found = ptrn.findPrefixMatchOf(job.getJobName) match {
          case Some(m) => m.group(1).toLong == taskId
          case None => false
        }

        if (found) {
          logger.info("Kill hadoop job: " + job.getID.toString)
          job.killJob
          return
        }
      }

      logger.info(s"Job not found for task id: $taskId")

    } finally {
      client.close
    }
  }

  private def removeComments(sql: String): String = {
    var result = sql;
    result = "(?s)/\\*.*?\\*/".r.replaceAllIn(result, "")
    result = "(?m)--.*$".r.replaceAllIn(result, "")
    result.trim
  }

  private def abridgeSql(sql: String): String = {

    var result = sql;

    // replace new line
    result = "[\\r\\n]+".r.replaceAllIn(result, " ")

    // remove buffer statements
    val ptrnBuffer = "(?i)^(SET|ADD\\s+JAR|CREATE\\s+TEMPORARY\\s+FUNCTION|USE)\\s+".r
    result = result.split(";").map(_.trim).filter(_.nonEmpty).filter(ptrnBuffer.findFirstIn(_).isEmpty).mkString("\\; ")

    result
  }

  def getConnection(serverKey: String): Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val server = Props.get(serverKey).openOrThrowException(s"$serverKey not found")
    DriverManager.getConnection(s"jdbc:hive2://$server", "hadoop", "")
  }

  def runQuery(conn: Connection, sql: String): (ResultSet, Int) = {

    val (bufferSqls, lastSql) = divideSql(sql)

    val stmt = conn.createStatement
    bufferSqls.foreach(stmt.execute)

    stmt.setFetchSize(FETCH_SIZE)
    stmt.execute(lastSql)

    (stmt.getResultSet, stmt.getUpdateCount)
  }

  def runQuery(sql: String, isInterrupted: () => Boolean)(implicit taskId: Long, conn: Conn): (ResultSet, Int) = {

    val (bufferSqls, lastSql) = divideSql(sql)

    val ptrnEngine = "^(?i:SET)\\s+dw\\.engine\\s*=\\s*(hive|shark)$".r
    val engine = bufferSqls.flatMap(ptrnEngine.findFirstMatchIn).map(_.group(1)).lastOption match {
      case Some("shark") if sharkable(lastSql) => "shark"
      case _ => "hive"
    }

    val connection = engine match {
      case "shark" => conn.shark
      case _ => conn.hive
    }

    val stmt = connection.createStatement
    bufferSqls.foreach(stmt.execute)

    stmt.setFetchSize(FETCH_SIZE)
    val resultFuture = Future {
      stmt.execute(lastSql)
      (stmt.getResultSet, stmt.getUpdateCount)
    }

    while (!isInterrupted()) {
      try {
        return Await.result(resultFuture, 1 second)
      } catch {
        case _: TimeoutException =>
      }
    }

    logger.info(s"Cancelling task id: $taskId")
    engine match {
      case "hive" => cleanupMapred
      case _ =>
    }

    /*
     * Since stmt.cancel is not implemented until Hive 0.13, the only way
     * to stop a HiveServer2 query is to kill the corresponding map-reduce job,
     * or we'll have to wait for it to complete.
     *
     * As for shark, not implemented yet.
     */
    try {
      logger.info(s"Waiting for cancelled task to complete, id: $taskId")
      Await.result(resultFuture, Duration.Inf)
    } catch {
      case e: Exception => throw new Exception("Task is interrupted.", e)
    }
  }

  private def divideSql(sql: String): (Seq[String], String) = {

    val sqls = sql.split(";").map(_.trim).filter(_.nonEmpty)
    if (sqls.isEmpty) {
      throw new Exception("SQL cannot be empty.")
    }

    (sqls.take(sqls.length - 1).toSeq, sqls.last)
  }

  private def sharkable(sql: String)(implicit conn: Conn): Boolean = {

    if (sql.matches("(?is)^(DROP|ALTER|LOAD).*")) {
      return false
    }

    if (sql.matches("(?is)^(CREATE|INSERT).*?PARTITION.*")) {
      return false
    }

    if (sql.matches("(?is)^CREATE.*?TABLE.*?JOIN.*")) {
      return false
    }

    if ("(?is)LEFT\\s+OUTER\\s+JOIN".r.findAllIn(sql).size > 1) {
      return false
    }

    if ("(?i)GET_JSON_OBJECT".r.findFirstIn(sql).nonEmpty) {
      return false
    }

    // test connection
    try {
        conn.shark
        true
    } catch {
      case _: Exception => false
    }
  }

  def fetchResult(rs: ResultSet): List[List[String]] = {

    val columnCount = rs.getMetaData.getColumnCount

    val rowIterator = new Iterator[List[String]] {
      def hasNext = rs.next
      def next = {
        for (i <- 1 to columnCount) yield optString(rs, i, "NULL")
      } toList
    }

    rowIterator.toList
  }

  private def optString(rs: ResultSet, i: Int, default: String = "") = {
    Option(rs.getString(i)) match {
      case Some(s) => s.trim
      case None => default
    }
  }

  def getColumns(rs: ResultSet): List[Column] = {
    val meta = rs.getMetaData
    val columns = for (i <- 1 to meta.getColumnCount) yield {
      Column(meta.getColumnLabel(i), meta.getColumnTypeName(i))
    }
    columns.toList
  }

}
