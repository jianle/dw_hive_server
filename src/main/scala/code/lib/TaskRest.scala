package code
package lib

import scala.io.Source
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import code.model.Task
import net.liftweb.common.{Loggable, Full, Empty}
import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import net.liftweb.mapper.ByList
import java.io.{File, FileInputStream, InputStream, ByteArrayInputStream}
import net.liftweb.http.StreamingResponse
import net.liftweb.util.Props

object TaskRest extends RestHelper with Loggable {

  val taskActor = DependencyFactory.inject[ActorRef]

  private def init() {

    // restore unfinished tasks
    val taskList = Task.findAll(
        ByList(Task.status, List(Task.STATUS_NEW, Task.STATUS_RUNNING)))

    taskList.map(task => {
      taskActor.map(_ ! task.id.get)
      logger.info("Restored task id " + task.id.get)
    })

  }

  init()

  serve("api" / "task" prefix {

    case "submit" :: Nil JsonPost json -> _ => {

      (json \ "query").extractOpt[String] match {

        case Some(query) =>
          val task = Task.create
            .query(query)
            .status(Task.STATUS_NEW)
            .saveMe()

          task.doPostCommit(() => taskActor.map(_ ! task.id.get))

          logger.info("Submitted task id " + task.id.get)

          ("status", "ok") ~ ("id", task.id.get)

        case None => ("status", "error") ~ ("msg", "Query cannot be empty.")
      }

    }

    case "status" :: taskId JsonGet _ => {

      val task = Task.find(taskId(0).toLong) openOr null

      if (task == null) {
        ("status", "error") ~ ("msg", "Task id not found.")
      } else {

        val taskStatus = task.status.get match {
          case Task.STATUS_NEW => "new"
          case Task.STATUS_RUNNING => "running"
          case Task.STATUS_OK => "ok"
          case Task.STATUS_ERROR => "error"
          case Task.STATUS_INTERRUPTED => "interrupted"
          case _ => "unknown"
        }

        if (taskStatus.equals("error")) {

          var errorMessage = ""

          try {
            val source = Source.fromFile(TaskActor.errorFile(task.id.get))
            errorMessage = source.getLines.mkString("\n")
            source.close
          } catch {
            case e: Exception => errorMessage = "Unable to get error message."
          }

          ("status", "ok") ~ ("taskStatus", taskStatus) ~ ("taskErrorMessage", errorMessage)
        } else {
          ("status", "ok") ~ ("taskStatus", taskStatus)
        }
      }

    }

    case "output" :: taskId JsonGet _ => {

      var stream: InputStream = null
      var size: Long = 0
      var errorMessage = ""

      val task = Task.find(taskId(0).toLong) openOr null
      if (task == null) {
        errorMessage = "Task id not found."
      } else {
        val file = new File(TaskActor.outputFile(task.id.get))
        try {
            stream = new FileInputStream(file)
            size = file.length
        } catch {
          case e: Exception => errorMessage = "Unable to get standard output."
        }
      }

      if (!errorMessage.isEmpty) {
        val data = ("error\n" + errorMessage).getBytes
        stream = new ByteArrayInputStream(data)
        size = data.length
      }

      StreamingResponse(data = stream,
                        onEnd = () => { stream.close },
                        size = size,
                        headers = ("content-type" -> "text/plain; charset=utf-8") :: Nil,
                        cookies = Nil,
                        code = 200)
    }

    case "cancel" :: taskId :: Nil JsonGet _ => {

      val task = Task.find(taskId.toLong) openOr null
      if (task == null) {
        ("status" -> "error") ~ ("msg" -> "Task id not found.")
      } else {

        val taskStatus = task.status.get
        if (taskStatus == Task.STATUS_NEW || taskStatus == Task.STATUS_RUNNING) {
          task.status(Task.STATUS_INTERRUPTED).save

          import dispatch._
          import dispatch.Defaults._
          import scala.sys.process._

          val jt = Props.get("hadoop.job.tracker") openOr "127.0.0.1:50030"
          val res = Http(url(s"http://$jt/jobtracker.jsp") OK as.String).map(result => {
            val ptrn = ">(job_[0-9]{12}_[0-9]+)</a>.*?<td id=\"name_[0-9]+\">\\[([0-9]+)\\]".r
            result.split("\n")
                .dropWhile(!_.contains("<h2 id=\"running_jobs\">"))
                .takeWhile(!_.contains("<hr>"))
                .map(ptrn.findFirstMatchIn(_))
                .filter(_.nonEmpty).map(_.get).headOption match {
                  case Some(m) if m.group(2).toLong == task.id.get =>
                    logger.info("Kill hadoop job: " + m.group(1))
                    Seq("hadoop", "job", "-kill", m.group(1)) !

                  case _ => 0
                }
          })
          res()
        }
        ("status" -> "ok") ~ Nil
      }
    }

  })

}
