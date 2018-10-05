package notebook.server.websocket

import akka.actor.ActorRef
import notebook.server.WebSockWrapper

/**
  * @author : tong.wang
  * @since : 10/2/18 8:45 PM
  * @version : 1.0.0
  */
trait WebSocketService {

  def register(ws: WebSockWrapper)

  def unregister(ws: WebSockWrapper)

  val socketActor: ActorRef
}
