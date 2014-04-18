package code
package lib

import scala.actors.threadpool.TimeUnit
import scala.actors.TIMEOUT
import scala.actors.Actor

object TaskService extends Actor {

  def act() {

    val taskActor = new TaskActor
    taskActor.start()

    loop {

      taskActor ! fetch()

      receiveWithin(5000) {
        case 'Fetch =>
        case 'Exit => {
          taskActor ! 'Exit
          exit()
        }
        case TIMEOUT =>
      }
    }
  }

  def fetch(): Long = {
    return System.currentTimeMillis()
  }

}
