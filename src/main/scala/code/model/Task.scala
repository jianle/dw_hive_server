package code.model

import net.liftweb.mapper._

class Task extends LongKeyedMapper[Task]
    with CreatedUpdated with IdPK {

  val STATUS_NEW = 1
  val STATUS_RUNNING = 2
  val STATUS_OK = 3
  val STATUS_ERROR = 4
  val STATUS_INTERRUPTED = 5
  val STATUS_PENDING = 6

  def getSingleton = Task

  object query extends MappedText(this)
  object prefix extends MappedString(this, 255)
  object status extends MappedInt(this)
}

object Task extends Task
    with LongKeyedMetaMapper[Task] {

  override def dbTableName = "hive_server_task"

  def isInterrupted(id: Long): Boolean = {
    Task.findAllFields(Seq(Task.status), By(Task.id, id)).headOption match {
      case Some(task) => task.status == Task.STATUS_INTERRUPTED
      case None => true
    }
  }

}
