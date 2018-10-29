package log

import java.io.File
import java.net.URL

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.slf4j.ILoggerFactory
import play.api.Mode.Mode
import play.api.{ Configuration, Environment, LoggerConfigurator, Mode }

/**
 * @author : tong.wang
 * @since : 10/11/18 10:32 PM
 * @version : 1.0.0
 */
class CustomLog4j2 extends LoggerConfigurator {
  private var factory: ILoggerFactory = _
  private val loggingConfigName = "log4j2.xml"

  override def init(rootPath: File, mode: Mode.Mode): Unit = {
    val properties = Map("application.home" -> rootPath.getAbsolutePath)
    val resourceName = if (mode == Mode.Dev) "log4j2-dev.xml" else "log4j2.xml"
    val resourceUrl = Option(this.getClass.getClassLoader.getResource(resourceName))
    configure(properties, resourceUrl)
  }

  override def shutdown(): Unit = {
    val context = LogManager.getContext().asInstanceOf[LoggerContext]
    Configurator.shutdown(context)
  }

  override def configure(env: Environment): Unit = {
    val properties = Map("application.home" -> env.rootPath.getAbsolutePath)
    val resourceUrl = env.resource(if (env.mode == Mode.Dev) "log4j2-dev.xml" else "log4j2.xml")
    configure(properties, resourceUrl)
  }

  override def configure(properties: Map[String, String], config: Option[URL]): Unit = {
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    context.setConfigLocation(config.get.toURI)
  }
}
