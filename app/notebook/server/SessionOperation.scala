package notebook.server

import akka.actor.ActorRef

/**
 * @author : tong.wang
 * @since : 10/2/18 5:13 PM
 * @version : 1.0.0
 */
case class SessionOperation(actor: ActorRef, cellId: Option[String])
