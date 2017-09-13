import akka.actor._
import akka.routing.{ ActorRefRoutee, RoundRobinRoutingLogic, Router }
import scala.concurrent.duration._

object Pi extends App {

  calculate(nrOfWorkers = 4, nrOfElements = 1000000, nrOfMessages = 1000)

  sealed trait PiMessage
  case object Calculate extends PiMessage
  case class Work(start: Long, nrOfElements: Int) extends PiMessage
  case class Result(value: Double) extends PiMessage
  case class PiApproximation(pi: Double, duration: Duration, steps: Long)
  
  class Worker extends Actor {
    
    def calculatePiFor(start: Long, nrOfElements: Int): Double = {
      var acc = 0.0
      for (i ← start until (start + nrOfElements))
      acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1)
      //println(s"worker calculating $start, $nrOfElements results = $acc")
      acc
    }

    def receive = {
      case Work(start, nrOfElements) =>
      sender().!(Result(calculatePiFor(start, nrOfElements))) // perform the work
    }

  }

  class Master(nrOfWorkers: Int, nrOfMessages: Int, nrOfElements: Int, listener: ActorRef) extends Actor {
    
    var pi: Double = _
    var nrOfResults: Int = _
    val start: Long = System.currentTimeMillis
    println("create router")
    var workerRouter = {

      val routees = Vector.fill(nrOfWorkers) {
        val r = context.actorOf(Props[Worker])
        context watch r
        ActorRefRoutee(r)
      }

      Router(RoundRobinRoutingLogic(), routees)
    }

    //println(s"router: $workerRouter")

    def receive = {
      case Calculate =>
        for (i <- 0 until nrOfMessages) workerRouter.route(Work((i * nrOfElements).toLong, nrOfElements), self) case Result(value) =>
        pi += value
        nrOfResults += 1
        //println(s"received value: $value")
        if (nrOfResults == nrOfMessages) {
          listener ! PiApproximation(pi, duration = (System.currentTimeMillis - start).millis, nrOfMessages.toLong * nrOfElements)
          context.stop(self)
        }

      case Terminated(a) =>
        workerRouter = workerRouter.removeRoutee(a)
        val r = context.actorOf(Props[Worker])
        context watch r
        workerRouter = workerRouter.addRoutee(r)
    }
  }

  class Listener extends Actor {
    def receive = {
      case PiApproximation(pi, duration, steps) =>
      println("\n\tPi approximation: \t%s\n\tCalculation time: \t%s\n\tSteps (millions): \t%s".format(pi, duration, steps/1000000))
      //ActorSystem.shutdown()
    }
  }

  def calculate(nrOfWorkers: Int, nrOfElements: Int, nrOfMessages: Int) {
    
    // Create an Akka system
    val system = ActorSystem("PiSystem")
    
    // create the result listener, which will print the result and shutdown the system
    val listener = system.actorOf(Props[Listener], name = "listener")
    
    // create the master
    val master = system.actorOf(Props(new Master(nrOfWorkers, nrOfMessages, nrOfElements, listener)), name = "master")
    println("start the calculation")
    master ! Calculate
  }

}
