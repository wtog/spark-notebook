package notebook.server.websocket

import akka.actor._
import notebook.client._
import notebook.repl.NameDefinition
import notebook.server._
import notebook.{ Kernel, ReplCollector }
import org.joda.time.LocalDateTime
import play.api._
import play.api.libs.json.Json.obj
import play.api.libs.json._

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.{ postfixOps, reflectiveCalls }

/**
 * Provides a web-socket interface to the Collector
 */
class CollWebSocketService(
  system: ActorSystem,
  notebookName: String,
  customImports: Option[List[String]],
  customArgs: Option[List[String]],
  initScripts: List[(String, String)],
  compilerArgs: List[String],
  kernel: Kernel,
  kernelTimeout: Option[Long]) extends WebSocketService {

  implicit val executor = system.dispatcher

  override val socketActor: ActorRef = system.actorOf(Props(new CollActor))

  def register(ws: WebSockWrapper) = socketActor ! Register(ws)

  def unregister(ws: WebSockWrapper) = socketActor ! Unregister(ws)

  class CollActor extends Actor with ActorLogging {
    private var currentSessionOperations: Queue[SessionOperation] = Queue.empty

    var collector: ActorRef = null
    var wss: List[WebSockWrapper] = Nil

    protected val notebookStartTime = LocalDateTime.now()

    private var lastCellExecutionTime: Option[LocalDateTime] = None

    private val lastTermDefinitions = mutable.Map[String, (NameDefinition, String)]()

    val ws = new {
      def send(header: JsValue, session: JsValue, msgType: String, channel: String,
        content: JsValue) = {
        Logger.trace(s"Sending info to websockets $wss in $this")
        wss.foreach { ws =>
          ws.send(header, ws.session, msgType, channel, content)
        }
      }
    }

    private def markNotebookAsActive() = {
      lastCellExecutionTime = Some(LocalDateTime.now)
    }

    protected def isKernelTimeouted = {
      val lastActionTime: LocalDateTime = lastCellExecutionTime match {
        case Some(lastExec) => lastExec
        case None => notebookStartTime
      }
      kernelTimeout.exists { kernelTimeoutMillis =>
        lastActionTime.plusMillis(kernelTimeoutMillis.toInt).isBefore(LocalDateTime.now())
      }
    }

    context.system.scheduler.schedule(initialDelay = 1.minutes, interval = 1.minutes) {
      if (isKernelTimeouted) {
        log.warning("Killing a timeouted kernel")
        collector ! PoisonPill
      }
    }

    private def spawnCalculator() {
      val kNotebookName = notebookName
      val kCompilerArgs = compilerArgs
      val kCustomImports = customImports
      val kCustomArgs = customArgs

      val kInitScripts = initScripts
      val remoteDeploy = Await.result(kernel.remoteDeployFuture, 2 minutes)

      collector = context.actorOf {
        Props(
          classOf[ReplCollector],
          kNotebookName,
          kCustomImports,
          kCustomArgs,
          self,
          kInitScripts,
          kCompilerArgs).withDeploy(remoteDeploy)
      }

      context.watch(collector)
    }

    def sendAllEarlierNameDefinitions(ws: WebSockWrapper): Unit = {
      lastTermDefinitions.values.foreach {
        case (definition, cellId) =>
          ws.send(
            obj(
              "session" → "ignored"),
            ws.session,
            "definition",
            "iopub",
            nameDefinitionJson(definition, cellId))
      }
      Logger.debug(s"Sent defs to ($ws) in service ${this} (current are: ${lastTermDefinitions.keys})")
    }

    def nameDefinitionJson(nameDefinition: NameDefinition, cellId: String): JsObject = {
      val NameDefinition(name, tpe, references) = nameDefinition
      val (te, ty): (JsValue, JsValue) = nameDefinition match {
        case d if !d.definesType => (JsString(name), JsNull)
        case _ => (JsNull, JsString(name))
      }
      obj(
        "term" → te,
        "type" → ty,
        "tpe" → tpe,
        "cell" → cellId,
        "references" → references)
    }

    def receive = {
      case Register(ws: WebSockWrapper) if wss.isEmpty && collector == null =>
        Logger.info(s"Registering first web-socket ($ws) in service ${this}")
        wss = List(ws)
        Logger.info(s"Spawning collector in service ${this}")
        spawnCalculator()

      case Register(ws: WebSockWrapper) =>
        Logger.info(s"Registering web-socket ($ws) in service ${this} (current count is ${wss.size})")
        wss = ws :: wss

      case Unregister(ws: WebSockWrapper) =>
        Logger.info(s"UN-registering web-socket ($ws) in service ${this} (current count is ${wss.size})")
        wss = wss.filterNot(_ == ws)

      case InterruptCell(killCellId) =>
        currentSessionOperations
          .filter { case SessionOperation(_, cellId) => cellId.contains(killCellId) }
          .foreach { op => collector.tell(InterruptCellRequest(killCellId), op.actor) }

      case InterruptCalculator =>
        Logger.info(s"Interrupting the computations, current is $currentSessionOperations")
        currentSessionOperations.headOption.foreach { op =>
          collector.tell(InterruptRequest, op.actor)
        }

        if (currentSessionOperations.tail.nonEmpty) {
          currentSessionOperations.tail.foreach {
            case SessionOperation(actor, _) =>
              actor ! StreamResponse("Previous cell has been interrupted", "stdout")
          }
        }

        currentSessionOperations = Queue.empty

      case WebUIReadyNotification(newlyStartedWs) =>
        sendAllEarlierNameDefinitions(newlyStartedWs)

      case req @ SessionRequest(header, session, request) =>
        val operations = new SessionOperationActors(header, session)
        val (operationActor, cellId) = (request: @unchecked) match {
          case ExecuteRequest(cellId, counter, code) =>
            markNotebookAsActive()
            ws.send(header, session, "status", "iopub", obj("execution_state" → "busy"))
            ws.send(header, session, "pyin", "iopub", obj("cell_id" → cellId, "execution_count" → counter, "code" → code))
            (operations.singleExecution(cellId, counter), Some(cellId))

          case _: CompletionRequest =>
            (operations.completion, None)

          case _: ObjectInfoRequest =>
            (operations.objectInfo, None)
        }
        val operation = context.actorOf(operationActor)
        context.watch(operation)
        currentSessionOperations = currentSessionOperations.enqueue(SessionOperation(operation, cellId))
        collector.tell(request, operation)

      //      case Terminated(actor) =>
      //        Logger.debug("Termination of op calculator")
      //        if (actor == collector) {
      //          Logger.error(s"Remote calculator ($collector) has been terminated !!!!!")
      //          kernel.shutdown()
      //
      //          ws.send(
      //            obj(
      //              "session" → "ignored"
      //            ),
      //            JsNull,
      //            "status",
      //            "iopub",
      //            obj("execution_state" → "dead")
      //          )
      //          self ! PoisonPill
      //        } else {
      //          Logger.debug(s"Termination of op calculator: ${currentSessionOperations.filter(_.actor == actor)}")
      //          currentSessionOperations = currentSessionOperations.filter(_.actor != actor)
      //        }

      //      case event:org.apache.log4j.spi.LoggingEvent =>
      ////         println("Received log event: " + s"""
      ////           > ${event.getLevel}
      ////           > ${event.getTimeStamp}
      ////           > ${event.getLoggerName}
      ////           > ${event.getMessage}
      ////         """)
      //        ws.send(
      //          obj(
      //            "session" → "ignored"
      //          ),
      //          JsNull,
      //          "log",
      //          "iopub",
      //          obj(
      //            "level"       → event.getLevel.toString,
      //            "time_stamp"  → event.getTimeStamp,
      //            "logger_name" → event.getLoggerName,
      //            "message"     → (""+Option(event.getMessage).map(_.toString).getOrElse("<no-message>")),
      //            "thrown"      → (if (event.getThrowableStrRep == null) List.empty[String] else event.getThrowableStrRep.toList)
      //          )
      //        )
    }

    class SessionOperationActors(header: JsValue, session: JsValue) {
      def singleExecution(cellId: String, counter: Int) = Props(new Actor {
        def receive = {
          case StreamResponse(data, name) =>
            ws.send(header, session, "stream", "iopub", obj("cell_id" → cellId, "text" → data, "name" → name))

          case ExecuteResponse(outputType, content, time) =>
            ws.send(header, session, "execute_result", "iopub", obj(
              "cell_id" → cellId,
              "execution_count" → counter,
              "data" → obj(outputType → content),
              "time" → time))
            ws.send(header, session, "status", "iopub", obj("cell_id" → cellId, "execution_state" → "idle"))
            ws.send(header, session, "execute_reply", "shell", obj("cell_id" → cellId, "execution_count" → counter))
            context.stop(self)

          case nameDefinition @ NameDefinition(name, tpe, references) =>
            lastTermDefinitions.put(name, (nameDefinition, cellId))
            Logger.debug(s"Registered a name definition. Current definitions: ${lastTermDefinitions.keys})")

            ws.send(
              obj(
                "session" → "ignored"),
              JsNull,
              "definition",
              "iopub",
              nameDefinitionJson(nameDefinition, cellId))

          case ErrorResponse(msg, incomplete) =>
            if (incomplete) {
              ws.send(header, session, "error", "iopub", obj(
                "execution_count" → counter,
                "status" → "error",
                "ename" → "Error",
                "traceback" → Seq(msg)))
            }

            ws.send(header, session, "status", "iopub", obj("execution_state" → "idle"))
            ws.send(header, session, "execute_reply", "shell", obj("execution_count" → counter))
            context.stop(self)
        }
      })

      def completion = Props(new Actor {
        def receive = {
          case CompletionResponse(cursorPosition, candidates, matchedText) =>
            ws.send(header, session, "complete_reply", "shell", obj(
              "matched_text" → matchedText,
              "matches" → candidates.map(_.toJsonWithDescription).toList,
              "cursor_start" → (cursorPosition - matchedText.length),
              "cursor_end" → cursorPosition))
            context.stop(self)
        }
      })

      def objectInfo = Props(new Actor {
        def receive = {
          case ObjectInfoResponse(found, name, callDef, callDocString) =>
            ws.send(header, session, "object_info_reply", "shell", obj(
              "found" → found,
              "name" → name,
              "call_def" → callDef,
              "call_docstring" → "Description TBD"))
            context.stop(self)
        }
      })
    }

  }

}
