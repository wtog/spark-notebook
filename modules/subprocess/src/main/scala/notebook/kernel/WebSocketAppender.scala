package notebook.kernel

import akka.actor.{Actor, ActorRef}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.appender.CountingNoOpAppender
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}

class WebSocketAppender extends Actor {

  var ref:Option[ActorRef] = None

  val log4j2Appender = Log4j2Appender("remote-actor-logger")

  case class Log4j2Appender(name: String) extends CountingNoOpAppender(name, PatternLayout.createDefaultLayout) {
    override def append(event: LogEvent): Unit = {
      println("log4j appender entered")
      ref foreach (_ ! event)
    }
  }

  def receive = {
    case r:ActorRef =>
      if (ref.isEmpty) {
        ref = Some(r)
        LogManager.getContext().asInstanceOf[LoggerContext].getRootLogger().addAppender(log4j2Appender)
      } else {
        ref = Some(r)
      }
  }

}

