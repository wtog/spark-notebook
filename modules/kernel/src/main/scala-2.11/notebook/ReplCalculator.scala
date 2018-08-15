package notebook
package client

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
import com.datafellas.utils._
import notebook.kernel._
import notebook.kernel.repl.common.{ReplHelpers, ReplT}
import notebook.repl.command_interpreters.combineIntepreters
import notebook.repl.{ReplCommand, command_interpreters}
import notebook.util.CoursierDeps
import org.joda.time.LocalDateTime
import org.sonatype.aether.repository.RemoteRepository

import scala.collection.immutable.Queue
import scala.concurrent.Future


/**
 * @param initScripts List of scala source strings to be executed during REPL startup.
 * @param customSparkConf Map configuring the notebook (spark configuration).
 * @param compilerArgs Command line arguments to pass to the REPL compiler
 */
class ReplCalculator(
  notebookName:String,
  customLocalRepo:Option[String],
  customRepos:Option[List[String]],
  customDeps:Option[List[String]],
  customImports:Option[List[String]],
  customArgs:Option[List[String]],
  customSparkConf:Option[Map[String, String]],
  remoteActor:ActorRef,
  initScripts: List[(String, String)],
  compilerArgs: List[String]
) extends Actor with akka.actor.ActorLogging {

  private val remoteLogger = context.actorSelection("/user/remote-logger")
  remoteLogger ! remoteActor

  private val authRegex = """(?s)^\s*\(([^\)]+)\)\s*$""".r
  private val credRegex = """"([^"]+)"\s*,\s*"([^"]+)"""".r //"

  private def remoreRepo(r:String):(String, RemoteRepository) = {
    val id::tpe::url::remaining = r.split("%").toList
    val rest = remaining.map(_.trim) match {
      case Nil => remaining
      case "maven"::r => r //skip the flavor → always maven in 2.11
      case xs => xs
    }
    val (username, password):(Option[String],Option[String]) = rest.headOption.map {
      case authRegex(usernamePassword) =>
        val (username, password) = usernamePassword match {
          case credRegex(username, password) => (username, password)
        }
        val u = if (username.startsWith("$")) sys.env.get(username.tail).get else username
        val p = if (password.startsWith("$")) sys.env.get(password.tail).get else password
        (Some(u), Some(p))
      case _ => (None, None)
    }.getOrElse((None, None))
    val rem = Repos(id.trim,tpe.trim,url.trim,username,password)
    val logR = r.replaceAll("\"", "\\\\\"")
    (logR, rem)
  }

  var remotes:List[RemoteRepository] = customRepos.getOrElse(List.empty[String]).map(remoreRepo _).map(_._2) :::
    List(Repos.mavenLocal, Repos.central, Repos.sparkPackages, Repos.oss)

  var repo:File = customLocalRepo.map { x =>
                    new File(notebook.util.StringUtils.updateWithVarEnv(x))
                  }.getOrElse {
                    val tmp = new File(System.getProperty("java.io.tmpdir"))

                    val snb = new File(tmp, "spark-notebook")
                    if (!snb.exists) snb.mkdirs

                    val aether = new File(snb, "aether")
                    if (!aether.exists) aether.mkdirs

                    val r = new File(aether, java.util.UUID.randomUUID.toString)
                    if (!r.exists) r.mkdirs

                    r
                  }

  def codeRepo = new File(repo, "code")

  val (depsJars, depsScript):(List[String],(String, ()=>String)) = customDeps.map { d =>
    val customDeps = d.mkString("\n")
    val deps = CoursierDeps.script(customDeps, remotes, repo, notebook.BuildInfo.xSparkVersion).toOption.getOrElse(List.empty[String])
    (deps, ("deps", () => s"""
                    |val CustomJars = ${ deps.mkString("Array(\"", "\",\"", "\")").replace("\\","\\\\") }
                    |
                    """.stripMargin))
  }.getOrElse((List.empty[String], ("deps", () => "val CustomJars = Array.empty[String]\n")))

  val ImportsScripts = ("imports", () => customImports.map(_.mkString("\n") + "\n").getOrElse("\n"))

  private var _repl:Option[ReplT] = None

  private def repl: ReplT = _repl getOrElse {
    val r = ReplT.create(compilerArgs, depsJars)
    _repl = Some(r)
    r
  }

  private var _presentationCompiler: Option[PresentationCompiler] = None

  private def presentationCompiler: PresentationCompiler = _presentationCompiler getOrElse {
    val r = new PresentationCompiler(depsJars)
    _presentationCompiler = Some(r)
    r
  }

  val chat = new notebook.front.gadgets.Chat()

  // Make a child actor so we don't block the execution on the main thread, so that interruption can work
  private val executor = context.actorOf(Props(new Actor {
    implicit val ec = context.dispatcher

    private var queue:Queue[(ActorRef, ExecuteRequest)] = Queue.empty
    private var currentlyExecutingTask: Option[Future[(String, EvaluationResult)]] = None

    def eval(b: => String, notify:Boolean=true)(success: => String = "", failure: String=>String=(s:String)=>"Error evaluating " + b + ": "+ s) {
      repl.evaluate(b)._1 match {
        case Failure(str) =>
          if (notify) {
            eval(s"""
            """,notify = false)()
          }
          log.error(failure(str))
        case _ =>
          if (notify) {
            eval(s"""
            """,notify = false)()
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

      case er@ExecuteRequest(_, _, code) =>
        log.debug("Enqueuing execute request at: " + queue.size)
        queue = queue.enqueue((sender(), er))

        // if queue contains only the new task, and no task is currently executing, execute it straight away
        // otherwise the execution will start once the evaluation of earlier cell(s) finishes
        if (currentlyExecutingTask.isEmpty && queue.size == 1) {
          self ! "process-next"
        }

      case InterruptCellRequest(killCellId) =>
        // kill job(s) still waiting for execution to start, if any
        val (jobsInQueueToKill, nonAffectedJobs) = queue.partition { case (_, ExecuteRequest(cellIdInQueue, _, _)) =>
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
            msg => thisSender ! StreamResponse(msg, "stdout")
          )
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
          }
        )
    }

    private var commandInterpreters = combineIntepreters(command_interpreters.defaultInterpreters)

    def execute(sender: ActorRef, er: ExecuteRequest): Unit = {
      val generatedReplCode: ReplCommand = commandInterpreters(er)
      val start = System.currentTimeMillis
      val thisSelf = self
      val thisSender = sender
      val result = scala.concurrent.Future {
        // this future is required to allow InterruptRequest messages to be received and process
        // so that spark jobs can be killed and the hand given back to the user to refine their tasks
        val cellId = er.cellId
        def replEvaluate(code:String, cellId:String) = {
          val cellResult = try {
           repl.evaluate(s"""
              |globalScope.sparkContext.setJobGroup("${JobTracking.jobGroupId(cellId)}", "${JobTracking.jobDescription(code, start)}")
              |$code
              """.stripMargin,
              msg => thisSender ! StreamResponse(msg, "stdout"),
              nameDefinition => thisSender ! nameDefinition
            )
          }
          finally {
             repl.evaluate("globalScope.sparkContext.clearJobGroup()")
          }
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
        case (timeToEval, notebook.kernel.Incomplete) =>
          thisSender ! ErrorResponse("Incomplete (hint: check the parenthesis)", incomplete = true)
      }

      result onComplete { _ =>
        thisSelf ! "process-next"
      }
    }
  }))

  def preStartLogic() {
    log.info("ReplCalculator preStart")

    val dummyScript = ("dummy", () => s"""val dummy = ();\n""")
    val SparkHookScript = ("class server", () => s"""@transient val _5C4L4_N0T3800K_5P4RK_HOOK = "${repl.classServerUri.get.replaceAll("\\\\", "\\\\\\\\")}";\n""")

    // Must escape last remaining '\', which could be for windows paths.
    val nbName = notebookName.replaceAll("\"", "").replace("\\", "\\\\")

    val SparkConfScript = {
      val m = customSparkConf .getOrElse(Map.empty[String, String])
      m .map{ case (k, v) =>
          "( \"" + k + "\"  → \"" + v + "\" )"
        }.mkString(",")
    }

    val CustomSparkConfFromNotebookMD = ("custom conf", () => s"""
      |@transient val notebookName = "$nbName"
      |@transient val _5C4L4_N0T3800K_5P4RK_C0NF:Map[String, String] = Map(
      |  $SparkConfScript
      |)\n
      """.stripMargin
    )

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

    val allInitScrips: List[(String, () => String)] = dummyScript :: SparkHookScript :: depsScript :: ImportsScripts :: CustomSparkConfFromNotebookMD :: initScripts.map(x => (x._1, () => x._2))
    for ((name, script) <- allInitScrips) {
      log.info(s" INIT SCRIPT: $name")
      eval(script).foreach { sc =>
        presentationCompiler.addScripts(sc)
      }
    }
  }

  override def preStart() {
    preStartLogic()
    super.preStart()
  }

  override def postStop() {
    log.info("ReplCalculator postStop")
    presentationCompiler.stop()
    super.postStop()
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.info("ReplCalculator preRestart " + message)
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable) {
    log.info("ReplCalculator postRestart")
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
          // REPL completions seem broken. but presentationCompiler finally +/- works in 2.11
          // val (matched, candidates) = repl.complete(line, cursorPosition)
          val (matched, candidates) = presentationCompiler.complete(line, cursorPosition)

          sender ! CompletionResponse(cursorPosition, candidates, matched)

        case ObjectInfoRequest(code, position) =>
          val completions = repl.objectInfo(code, position)

          val resp = if (completions.length == 0) {
            ObjectInfoResponse(false, code, "", "")
          } else {
            ObjectInfoResponse(true, code, completions.mkString("\n"), "")
          }

          sender ! resp
      }
  }
}
