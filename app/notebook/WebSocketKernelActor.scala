package notebook

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import notebook.client._
import notebook.server._
import notebook.server.websocket.WebSocketService
import play.api.libs.iteratee.Concurrent
import play.api.libs.json._

object WebSocketKernelActor {
  def props(
    channel: Concurrent.Channel[JsValue],
    webSocketService: WebSocketService,
    session_id: String)(implicit system: ActorSystem): ActorRef = {
    system.actorOf(Props(new WebSocketKernelActor(channel, webSocketService, session_id)))
  }
}

class WebSocketKernelActor(
  channel: Concurrent.Channel[JsValue],
  val webSocketService: WebSocketService,
  session_id: String)(implicit system: ActorSystem) extends Actor with akka.actor.ActorLogging {

  private lazy val executionCounter = new AtomicInteger(0)
  private lazy val ws = new WebSockWrapperImpl(channel, session_id)

  override def preStart() = {
    webSocketService.register(ws)
  }

  override def postStop() = {
    webSocketService.unregister(ws)
  }

  /**
   * process the akka message from web
   * the whole invoke path is:
   * <pre>
   * +-----------+        +----------+                  +--------------+            +------------+
   * |session.js |  --->  | kernel.js|    websockets    | route->Action|    --->    | ActorSystem|
   * |api/xxxx   |  --->  | api/xxxx |    --------->    | Application  |    --->    |    this    |
   * +-----------+        +----------+                  +--------------+            +------------+
   * </pre>
   *
   * @return
   */
  def receive = {
    case json: JsValue =>
      val header = (json \ "header").get
      val session = (header \ "session").get
      val msgType = header \ "msg_type"
      val content = json \ "content"

      msgType.get match {
          // This msg is sent after Browser Kernel/session is ready, should do well for init
        case JsString("kernel_info_request") =>
          ws.send(header, session_id, "info", "shell",
            Json.obj(
              "language_info" -> Json.obj(
                "name" → "scala",
                "file_extension" → "scala",
                "codemirror_mode" → "text/x-scala"
              ),
              "extension" → "scala"
            )
          )
          webSocketService.socketActor() ! WebUIReadyNotification(ws)
        case JsString("interrupt_cell_request") =>
          val JsString(cellId) = (content \ "cell_id").get
          webSocketService.socketActor() ! InterruptCell(cellId)
        case JsString("interrupt_request") =>
          webSocketService.socketActor() ! InterruptCalculator
        case JsString("execute_request") =>
          val JsString(cellId) = (content \ "cell_id").get
          val JsString(code) = (content \ "code").get
          val execCounter = executionCounter.incrementAndGet()
          webSocketService.socketActor() ! SessionRequest(header, session, ExecuteRequest(cellId, execCounter, code))
        case JsString("complete_request") =>
          val JsString(line) = (content \ "code").get
          val JsNumber(cursorPos) = (content \ "cursor_pos").get
          webSocketService.socketActor() ! SessionRequest(header, session, CompletionRequest(line, cursorPos.toInt))
        case JsString("inspect_request") =>
          val JsString(code) = (content \ "code").get
          val JsNumber(position) = (content \ "cursor_pos").get
          val JsNumber(detailLevel) = (content \ "detail_level").get
          webSocketService.socketActor() ! SessionRequest(header, session, ObjectInfoRequest(code, position.toInt))
        case x => log.warning("Unrecognized websocket message: " + json)
      }
  }

}