package notebook.front.gadgets

class Chat() {

  val connection = notebook.JSBus.createConnection("chat")

  connection --> notebook.Connection.fromObserver { msg =>
    connection <-- notebook.Connection.just(msg)
  }

}