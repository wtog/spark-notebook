package notebook

/**
 * Author: Ken
 * An observer and observable -  mutable data that can be changed or subscribed to
 */
trait Connection[T] extends notebook.util.Logging {

  def observable: Observable[T]

  def observer: Observer[T]

  def biMap[A](
    codec: Codec[T, A]): Connection[A] = new MappingConnection[T, A](Connection.this, codec)

  def biMap[A](aToB: A => T, bToA: T => A): Connection[A] = biMap[A](new Codec[T, A] {
    def encode(x: T) = bToA(x)

    def decode(x: A) = aToB(x)
  })

  def <--(other: Connection[T]) {
    other.observable.subscribe(observer)
  }

  def -->(other: Connection[T]) {
    observable.subscribe(other.observer)
  }

  def <-->(other: Connection[T]) {
    this <-- other
    this --> other
  }
}

class ConcreteConnection[T](
  val observable: Observable[T],
  val observer: Observer[T]) extends Connection[T]

object Connection {
  def just[T](v: T) = new ConcreteConnection[T](Observable.just(v), new NoopObserver[T]())

  def fromObserver[T](f: T => Unit) = new ConcreteConnection(Observable.noop, Observer(f))

  def fromObservable[T](observable: Observable[T]) = new ConcreteConnection(observable, new NoopObserver[T]())

}

class MappingConnection[A, B](innerConn: Connection[A], codec: Codec[A, B]) extends Connection[B] {
  val observable = new MappingObservable[A, B] {
    protected def innerObservable = innerConn.observable

    protected def observableMapper = codec.encode
  }

  val observer = new MappingObserver[A, B] {
    protected def innerObserver = innerConn.observer

    protected def observerMapper = codec.decode
  }
}