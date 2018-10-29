package notebook.kernel.pfork

import java.io.{ EOFException, File, ObjectInputStream, ObjectOutputStream }
import java.net._
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.Config
import org.apache.commons.exec._
import org.apache.commons.exec.util.StringUtils
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import play.api.{ Logger, Play }

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration.Duration

trait ForkableProcess {
  /**
   * Called in the remote VM. Can return any useful information to the server through the return
   * @param args
   * @return
   */
  def init(args: Seq[String]): String

  def waitForExit()
}

/**
 * I am so sick of this being a thing that gets implemented everywhere. Let's abstract.
 */
class BetterFork[A <: ForkableProcess: reflect.ClassTag](config: Config, executionContext: ExecutionContext, customArgs: Option[List[String]]) {

  private implicit val ec = executionContext

  import BetterFork._

  val processClass = (implicitly[reflect.ClassTag[A]]).runtimeClass

  def workingDirectory = new File(if (config.hasPath("wd")) config.getString("wd") else ".")

  def heap: Long = if (config.hasPath("heap")) config.getBytes("heap") else defaultHeap

  def stack: Long = if (config.hasPath("stack")) config.getBytes("stack") else -1

  def permGen: Long = if (config.hasPath("permGen")) config.getBytes("permGen") else -1

  def reservedCodeCache: Long = if (config.hasPath("reservedCodeCache")) config.getBytes("reservedCodeCache") else -1

  def server: Boolean = true

  def debugPort: Option[Int] = if (config.hasPath("debug.port")) Some(config.getInt("debug.port")) else None

  def logLevel: String = if (config.hasPath("log.level")) config.getString("log.level") else "info"

  def vmArgs: List[String] = if (config.hasPath("vmArgs")) config.getStringList("vmArgs").toList else Nil

  def classPathEnv = Array(
    sys.env.get("YARN_CONF_DIR"),
    sys.env.get("HADOOP_CONF_DIR"),
    sys.env.get("EXTRA_CLASSPATH")).collect { case Some(x) => x }

  def classPath: IndexedSeq[String] =
    if (config.hasPath("classpath")) config.getStringList("classpath").toList.toVector else Vector.empty[String]

  def classPathString = (defaultClassPath ++ classPath ++ classPathEnv).mkString(File.pathSeparator)

  def jvmArgs = {
    val builder = IndexedSeq.newBuilder[String]

    def ifNonNeg(value: Long, prefix: String) {
      if (value >= 0) {
        builder += (prefix + value)
      }
    }

    ifNonNeg(heap, "-Xmx")
    ifNonNeg(stack, "-Xss")
    ifNonNeg(permGen, "-XX:MaxPermSize=")
    ifNonNeg(reservedCodeCache, "-XX:ReservedCodeCacheSize=")

    if (server) builder += "-server"
    debugPort.foreach { p =>
      builder ++= IndexedSeq("-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=" + p)
    }

    builder ++= vmArgs

    customArgs.foreach(as => builder ++= as)

    builder.result()
  }

  implicit protected def int2SuffixOps(i: Int) = new SuffixOps(i)

  protected final class SuffixOps(i: Int) {
    def k: Long = i.toLong << 10

    def m: Long = i.toLong << 20

    def g: Long = i.toLong << 30
  }

  def execute(args: String*): Future[ProcessInfo] = {
    /* DK: Bi-directional liveness can be detected via redirected System.in (child), System.out (parent), avoids need for socket... */
    val ss = new ServerSocket(0)
    val cmd = new CommandLine(javaHome + "/bin/java")
      .addArguments(jvmArgs.toArray)
      .addArgument(classOf[ChildProcessMain].getName)
      .addArgument(processClass.getName)
      .addArgument(ss.getLocalPort.toString)
      .addArgument(logLevel)
      .addArguments(args.toArray)

    Future {
      log.info("Spawning %s".format(cmd.toString))
      // use environment because classpaths can be longer here than as a command line arg
      val environment = System.getenv + ("CLASSPATH" -> classPathString)
      val exec = new KillableExecutor

      val completion = Promise[Int]()
      exec.setWorkingDirectory(workingDirectory)
      exec.execute(cmd, environment, new ExecuteResultHandler {
        Logger.trace(s"With Env $environment")
        Logger.info(s"In working directory $workingDirectory")

        def onProcessFailed(e: ExecuteException) {
          Logger.error(e.getMessage)
          e.printStackTrace()
        }

        def onProcessComplete(exitValue: Int) {
          completion.success(exitValue)
        }
      })
      val socket = ss.accept()
      serverSockets += socket
      try {
        // we're waiting for RemoteActorProcess.init to write the address of the remote actor system on the socket
        val ois = new ObjectInputStream(socket.getInputStream)
        val address = ois.readObject().asInstanceOf[String]
        new ProcessInfo(() => exec.kill(), address, completion.future)
      } catch {
        case ex: SocketException => throw new ExecuteException("Failed to start process %s".format(cmd), 1, ex)
        case ex: EOFException => throw new ExecuteException("Failed to start process %s".format(cmd), 1, ex)
      }
    }
  }
}

class ProcessInfo(killer: () => Unit, val initReturn: String, val completion: Future[Int]) {
  def kill() {
    killer()
  }
}

object BetterFork {

  private val serverSockets = new ListBuffer[Socket]()

  def defaultClassPath: IndexedSeq[String] = {
    def urls(cl: ClassLoader, acc: IndexedSeq[String] = IndexedSeq.empty): IndexedSeq[String] = {
      if (cl != null) {
        val us = if (!cl.isInstanceOf[URLClassLoader]) {
          acc
        } else {
          acc ++ (cl.asInstanceOf[URLClassLoader].getURLs map { u =>
            val f = new File(u.getFile)
            URLDecoder.decode(f.getAbsolutePath, "UTF8")
          })
        }
        urls(cl.getParent, us)
      } else {
        acc
      }
    }
    val loader = Play.current.classloader
    val gurls = urls(loader).distinct.filter(!_.contains("logback-classic"))
    gurls
  }

  def defaultHeap = Runtime.getRuntime.maxMemory

  /* Override to expose ability to forcibly kill the process */
  private class KillableExecutor extends DefaultExecutor {
    val killed = new AtomicBoolean(false)
    setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT) {
      override def start(p: Process) {
        if (killed.get()) p.destroy()
      }
    })

    def kill() {
      if (killed.compareAndSet(false, true))
        Option(getExecutorThread) foreach (_.interrupt())
    }
  }

  private lazy val javaHome = System.getProperty("java.home")

  private lazy val log = LoggerFactory.getLogger(getClass)

  private[pfork] def main(args: Array[String]): Unit = {
    val className = args(0)
    val parentPort = args(1).toInt
    val logLevel = args(2)
    val kernelId = args(3)
    val path = args(4)
    // P.S. this arg is not passed to doMain
    val maybeProxyUser = args(5) match {
      case "NONE" => None
      case userName => Some(userName)
    }
    val remainingArgs = args.drop(6).toIndexedSeq

    maybeProxyUser match {
      case None =>
        doMain(className, parentPort, logLevel, kernelId, path, remainingArgs)

      case Some(proxyUserName) =>
        println(s"Impersonating the REPL as user: $proxyUserName")
        // based on code from spark-submit, see https://github.com/apache/spark/pull/4405/files
        // and https://github.com/apache/spark/blob/c622a87c/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L154-L180
        import org.apache.hadoop.security.UserGroupInformation
        import java.security.PrivilegedExceptionAction
        val proxyUser = UserGroupInformation.createProxyUser(proxyUserName, UserGroupInformation.getCurrentUser())
        try {
          proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              doMain(className, parentPort, logLevel, kernelId, path, remainingArgs)
            }
          })
        } catch {
          case e: Exception =>
            // Hadoop's AuthorizationException suppresses the exception's stack trace, which
            // makes the message printed to the output by the JVM not very helpful. Instead,
            // detect exceptions with empty stack traces here, and treat them differently.
            if (e.getStackTrace().length == 0) {
              // scalastyle:off println
              // FIXME: printStream.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
              println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
              // scalastyle:on println
              // FIXME: exitFn(1)
              throw e
            } else {
              throw e
            }
        }
    }
  }

  private[pfork] def doMain(className: String, parentPort: Int, logLevel: String, kernelId: String, path: String, remainingArgs: Seq[String]): Unit = {
    val propLog = new java.util.Properties()
    propLog.load(getClass().getResourceAsStream("/log4j.subprocess.properties"))
    val cleanPath = path.replaceAll("/", "\\\\").replaceAll("\"", "").replaceAll("'", "")

    propLog.setProperty("log4j.appender.rolling.File", s"logs/sn-session-$kernelId-$cleanPath.log")
    propLog.setProperty("log4j.rootLogger", s"$logLevel, rolling")

    PropertyConfigurator.configure(propLog)

    log.info("Remote process starting")
    val socket = new Socket("127.0.0.1", parentPort)

    val hostedClass = Class.forName(className).newInstance().asInstanceOf[ForkableProcess]

    val result = hostedClass.init(remainingArgs)

    val oos = new ObjectOutputStream(socket.getOutputStream)
    oos.writeObject(result)
    oos.flush()

    val executorService = Executors.newFixedThreadPool(10)
    implicit val ec = ExecutionContext.fromExecutorService(executorService)

    val parentDone = Future {
      socket.getInputStream.read()
    }
    val localDone = Future {
      hostedClass.waitForExit()
    }

    val done = Future.firstCompletedOf(Seq(parentDone, localDone))

    try {
      Await.result(done, Duration.Inf)
    } finally {
      log.warn("Parent process stopped; exiting.")
      sys.exit(0)
    }
  }
}
