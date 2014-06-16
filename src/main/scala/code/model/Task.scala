package code
package model

import net.liftweb.mapper.MappedInt
import net.liftweb.mapper.MappedString
import net.liftweb.mapper.LongKeyedMapper
import net.liftweb.mapper.LongKeyedMetaMapper
import net.liftweb.mapper.IdPK
import net.liftweb.mapper.CreatedUpdated
import net.liftweb.mapper.MappedText

class Task extends LongKeyedMapper[Task]
    with CreatedUpdated with IdPK {

  val STATUS_NEW = 1
  val STATUS_RUNNING = 2
  val STATUS_OK = 3
  val STATUS_ERROR = 4
  val STATUS_INTERRUPTED = 5

  def getSingleton = Task

  object query extends MappedText(this)
  object status extends MappedInt(this)
}

object Task extends Task
    with LongKeyedMetaMapper[Task] {

  override def dbTableName = "hive_server_task"
}
