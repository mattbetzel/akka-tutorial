package akka.tutorial.first.scala

import akka.actor.{Actor, ActorRef, Channel, PoisonPill}
import Actor._
import akka.routing.{Routing, CyclicIterator}
import Routing._
import akka.dispatch.{Dispatchers, Future}
import akka.util.duration._
import akka.util.Duration
import akka.actor.Scheduler

import java.util.concurrent.TimeUnit.MILLISECONDS

sealed trait PiMessage

case object Calculate extends PiMessage

case class Work(start: Int, nrOfElements: Int) extends PiMessage

case class Result(value: Double) extends PiMessage

case class CalcResult(value: Double, time: Duration) extends PiMessage

class Worker extends Actor {
  def receive = {
    case Work(start, nrOfElements) =>
      self reply Result(calculatePiFor(start, nrOfElements))
  }

  def calculatePiFor(start: Int, nrOfElements: Int) : Double = {
    def calculatePi(n: Int) : Double = 4.0 * (1 - (n % 2) * 2) / (2 * n + 1)

    start.until(start + nrOfElements).map(calculatePi).sum
  }
}

class Master(nrOfWorkers: Int, nrOfMessages: Int, nrOfElements: Int) extends Actor {
  import scala.collection.immutable.Seq
  val workers = Seq.fill(nrOfWorkers)(actorOf[Worker].start()) 
  val router = Routing.loadBalancerActor(CyclicIterator(workers)).start()

  def receive = {
    case Calculate => {
      val start = System.currentTimeMillis

      val resultFutures : Seq[Future[Result]] = 0.until(nrOfMessages).map { i =>
        router.?(Work(i * nrOfElements, nrOfElements))(timeout = 10 seconds)
      }.asInstanceOf[Seq[Future[Result]]]
      
      mapFutures(resultFutures, start) onComplete pipeTo(self.channel)
    }
  }

  private def mapFutures(resultFutures: Seq[Future[Result]], start: Long) : Future[CalcResult] =
    Future.sequence(resultFutures, 10000).map { results =>
      CalcResult(results.map(_.value).sum, durationFrom(start))
    }

  private def pipeTo(c: Channel[Any]) : Future[Any] => Unit =
    (f: Future[Any]) => f.value.get.fold(c.sendException(_), c.tell(_))

  private def durationFrom(start: Long) : Duration =
    Duration(System.currentTimeMillis - start, MILLISECONDS)

  
  override def postStop() {
    router ! Broadcast(PoisonPill)
    router ! PoisonPill
  }
}

object Pi extends App {
  calculate(nrOfWorkers = 4, nrOfElements = 10000, nrOfMessages = 10000)

  def calculate(nrOfWorkers: Int, nrOfElements: Int, nrOfMessages: Int) {
    val master = actorOf(new Master(nrOfWorkers, nrOfMessages, nrOfElements)).start()

    val result = master.?(Calculate)(timeout = 10 seconds)

    println(result.map {
      case CalcResult(value, duration) =>
        "\n\tPi estimate: \t\t%s\n\tCalculation time: \t%s millis".format(value, duration)
    }.get)

    master ! PoisonPill
  }
}
