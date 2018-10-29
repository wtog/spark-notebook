package notebook

import rx.lang.scala.{ Observable => RxObservable, Observer => RxObserver, _ }

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Author: Ken
 * An observer trait, which is unfortunately lacking in rx.Subscription
 */
trait Observable[T] {
  def inner: RxObservable[T]

  def subscribe(observer: Observer[T]): Subscription

  def map[A](fxn: T => A): Observable[A] = new Observable[A] {
    def inner = Observable.this.inner.map(fxn)

    def subscribe(observer: Observer[A]) = Observable.this.subscribe(observer map fxn)
  }
}

class WrappedObservable[T](val inner: RxObservable[T]) extends Observable[T] {
  def subscribe(observer: Observer[T]) = inner.subscribe(observer)
}

trait MappingObservable[A, B] extends Observable[B] {
  protected def innerObservable: Observable[A]

  protected def observableMapper: A => B

  override lazy val inner: RxObservable[B] = innerObservable.inner.map(observableMapper)

  def subscribe(observer: Observer[B]) = innerObservable.subscribe(observer map observableMapper)
}

object Observable {
  def noop[T]: Observable[T] = new WrappedObservable[T](RxObservable.never)

  def just[T](x: T): Observable[T] = new WrappedObservable[T](RxObservable.just(x))

  def from[T](f: Future[T]): Observable[T] = new WrappedObservable(RxObservable.from(f))
}