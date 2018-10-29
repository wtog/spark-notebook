package notebook

import akka.actor.{ Actor, ActorRef, Props }
import com.datafellas.utils._
import notebook.client._
import notebook.repl.command_interpreters.combineIntepreters
import notebook.repl._
import org.joda.time.LocalDateTime
import org.sonatype.aether.repository.RemoteRepository

import scala.collection.immutable.Queue
import scala.concurrent.Future

/**
 * @param initScripts List of scala source strings to be executed during REPL startup.
 * @param customSparkConf Map configuring the notebook (spark configuration).
 * @param compilerArgs Command line arguments to pass to the REPL compiler
 */
class ReplCollector(
  notebookName: String,
  customImports: Option[List[String]],
  customArgs: Option[List[String]],
  remoteActor: ActorRef,
  initScripts: List[(String, String)],
  compilerArgs: List[String]) extends Actor with akka.actor.ActorLogging {

  private val remoteLogger = context.actorSelection("/user/remote-logger")
  remoteLogger ! remoteActor

  private val authRegex = """(?s)^\s*\(([^\)]+)\)\s*$""".r
  private val credRegex = """"([^"]+)"\s*,\s*"([^"]+)"""".r //"

  val ImportsScripts = ("imports", () => customImports.map(_.mkString("\n") + "\n").getOrElse("\n"))

  private lazy val repl = ReplT.create(replClazzPath = "io.github.wtog.collector.Repl", compilerArgs)

  private val executor = context.actorOf(Props(new Actor {
    implicit val ec = context.dispatcher

    private var queue: Queue[(ActorRef, ExecuteRequest)] = Queue.empty
    private var currentlyExecutingTask: Option[Future[(String, EvaluationResult)]] = None

    def eval(b: => String, notify: Boolean = true)(success: => String = "", failure: String => String = (s: String) => "Error evaluating " + b + ": " + s) {
      repl.evaluate(b)._1 match {
        case Failure(str) =>
          if (notify) {
            eval(s"""
            """, notify = false)()
          }
          log.error(failure(str))
        case _ =>
          if (notify) {
            eval(s"""
            """, notify = false)()
          }
          log.info(success)
      }
    }

    def receive = {
      case "process-next" =>
        log.debug(s"Processing next asked, queue is ${queue.size} length now")
        currentlyExecutingTask = None
        if (queue.nonEmpty) { //queue could be empty if InterruptRequest was asked!
          log.debug("Dequeuing execute request current size: " + queue.size)
          val (executeRequest, queueTail) = queue.dequeue
          queue = queueTail
          val (ref, er) = executeRequest
          log.debug("About to execute request from the queue")
          execute(ref, er)
        }

      case er @ ExecuteRequest(_, _, code) =>
        log.debug("Enqueuing execute request at: " + queue.size)
        queue = queue.enqueue((sender(), er))
        // if queue contains only the new task, and no task is currently executing, execute it straight away
        // otherwise the execution will start once the evaluation of earlier cell(s) finishes
        if (currentlyExecutingTask.isEmpty && queue.size == 1) {
          self ! "process-next"
        }

      case InterruptCellRequest(killCellId) =>
        // kill job(s) still waiting for execution to start, if any
        val (jobsInQueueToKill, nonAffectedJobs) = queue.partition {
          case (_, ExecuteRequest(cellIdInQueue, _, _)) =>
            cellIdInQueue == killCellId
        }
        log.debug(s"Canceling $killCellId jobs still in queue (if any):\n $jobsInQueueToKill")
        queue = nonAffectedJobs
        log.debug(s"Interrupting the cell: $killCellId")
        val jobGroupId = JobTracking.jobGroupId(killCellId)
        // make sure sparkContext is already available!
        if (jobsInQueueToKill.isEmpty && repl.sparkContextAvailable) {
          log.info(s"Killing job Group $jobGroupId")
          val thisSender = sender()
          repl.evaluate(
            s"""globalScope.sparkContext.cancelJobGroup("${jobGroupId}")""",
            msg => thisSender ! StreamResponse(msg, "stdout"))
        }
        // StreamResponse shows error msg
        sender() ! StreamResponse(s"The cell was cancelled.\n", "stderr")
        // ErrorResponse to marks cell as ended
        sender() ! ErrorResponse(s"The cell was cancelled.\n", incomplete = false)

      case InterruptRequest =>
        log.debug("Interrupting the spark context")
        val thisSender = sender()
        log.debug("Clearing the queue of size " + queue.size)
        queue = scala.collection.immutable.Queue.empty
        repl.evaluate(
          "globalScope.sparkContext.cancelAllJobs()",
          msg => {
            thisSender ! StreamResponse(msg, "stdout")
          })
    }

    private var commandInterpreters = combineIntepreters(command_interpreters.defaultInterpreters)

    def execute(sender: ActorRef, er: ExecuteRequest): Unit = {
      val generatedReplCode: ReplCommand = commandInterpreters(er)

      println(s"collector generatedReplCode: ${generatedReplCode}")
      val start = System.currentTimeMillis
      val thisSelf = self
      val thisSender = sender
      val result = scala.concurrent.Future {
        val cellId = er.cellId
        def replEvaluate(code: String, cellId: String) = {
          val cellResult = try {
            repl.evaluate(
              s"""
              |$code
              """.stripMargin,
              msg => thisSender ! StreamResponse(msg, "stdout"),
              nameDefinition => thisSender ! nameDefinition)
          }
          //          finally {
          //             repl.evaluate("globalScope.sparkContext.clearJobGroup()")
          //          }
          cellResult
        }
        val result = replEvaluate(generatedReplCode.replCommand, cellId)
        val d = ReplHelpers.formatShortDuration(durationMillis = System.currentTimeMillis - start)
        (d, result._1)
      }
      currentlyExecutingTask = Some(result)

      result foreach {
        case (timeToEval, Success(result)) =>
          val evalTimeStats = s"Took: $timeToEval, at ${new LocalDateTime().toString("Y-MM-dd HH:mm")}"
          thisSender ! ExecuteResponse(generatedReplCode.outputType, result.toString, evalTimeStats)
        case (timeToEval, Failure(stackTrace)) =>
          thisSender ! ErrorResponse(stackTrace, incomplete = false)
        case (timeToEval, Incomplete) =>
          thisSender ! ErrorResponse("Incomplete (hint: check the parenthesis)", incomplete = true)
      }

      result onComplete { _ =>
        thisSelf ! "process-next"
      }
    }
  }))

  def preStartLogic() {
    log.info("RepCollector preStart")

    val dummyScript = ("dummy", () => s"""val dummy = ();\n""")

    def eval(script: () => String): Option[String] = {
      val sc = script()
      log.debug("script is :\n" + sc)
      if (sc.trim.length > 0) {
        val (result, _) = repl.evaluate(sc)
        result match {
          case Failure(str) =>
            log.error("Error in init script: \n%s".format(str))
            None
          case _ =>
            if (log.isDebugEnabled) log.debug("\n" + sc)
            log.info("Init script processed successfully")
            Some(sc)
        }
      } else None
    }

    val allInitScrips: List[(String, () => String)] = dummyScript :: ImportsScripts :: initScripts.map(x => (x._1, () => x._2))
    for ((name, script) <- allInitScrips) {
      log.info(s" INIT SCRIPT: $name")
      eval(script).foreach { sc =>
      }
    }
  }

  override def preStart() {
    preStartLogic()
    super.preStart()
  }

  override def postStop() {
    log.info("RepCollector postStop")
    super.postStop()
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.info("RepCollector preRestart " + message)
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable) {
    log.info("RepCollector postRestart")
    reason.printStackTrace()
    super.postRestart(reason)
  }

  def receive = {
    case msgThatShouldBeFromTheKernel =>
      msgThatShouldBeFromTheKernel match {
        case req @ InterruptCellRequest(_) =>
          executor.forward(req)

        case InterruptRequest =>
          executor.forward(InterruptRequest)

        case req @ ExecuteRequest(_, _, code) => executor.forward(req)

        case CompletionRequest(line, cursorPosition) =>
          sender ! CompletionResponse(cursorPosition, matchedText = "")

        case ObjectInfoRequest(code, position) =>
          val completions = repl.objectInfo(code, position)

          val resp = if (completions.isEmpty) {
            ObjectInfoResponse(found = false, code, "", "")
          } else {
            ObjectInfoResponse(found = true, code, completions.mkString("\n"), "")
          }

          sender ! resp
      }
  }
}
