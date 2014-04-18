package code
package lib

import scala.actors.Actor
import scala.actors.SchedulerAdapter
import scala.actors.threadpool.Executors

class TaskActor extends Actor {

  val pool = Executors.newFixedThreadPool(20)

  override def scheduler = new SchedulerAdapter {
    def execute(block: => Unit) =
      pool.execute(new Runnable {
        def run() { block }
      })
  }

  def act() {
    loop {
      react {
        case ts: Long => println(ts)
        case 'Exit => exit()
      }
    }
  }

}
