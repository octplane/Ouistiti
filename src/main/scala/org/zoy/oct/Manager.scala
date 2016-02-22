package org.zoy.oct

import akka.actor.Actor
import akka.actor.Props
import com.mongodb.casbah.Imports._
import com.typesafe.scalalogging.LazyLogging

import com.github.nscala_time.time.Imports._
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers


object TwittProcessor {

  case class FollowAndRT(userId: Long, statusId: Long)

  val following = RemoteServices.mongoConn("ouiouistiti")("following")
  val RTed = RemoteServices.mongoConn("ouiouistiti")("retweeted")
}


class TwittProcessor extends Actor with LazyLogging {
  RegisterJodaTimeConversionHelpers.register()

  def receive = {
    case TwittProcessor.FollowAndRT(userId, statusId) => {
      val user = TwittProcessor.following.findOne(MongoDBObject("_id" -> userId))
      user match {
        case Some(u) => logger.info(s"Already following $userId")
        case None => {
          logger.info(s"Following $userId")
          RemoteServices.twitter.friendsFollowers.createFriendship(userId)
          val obj = MongoDBObject("_id" -> userId, "followDate" -> DateTime.now)
          TwittProcessor.following += obj
        }
      }

      if (TwittProcessor.RTed.find(MongoDBObject("_id" -> statusId)).count == 1) {
        logger.info(s"Already Retweeted $statusId")
      } else {
        logger.info(s"RTing $statusId")
        RemoteServices.twitter.tweets.retweetStatus(statusId)
        val obj = MongoDBObject("_id" -> statusId, "rtDate" -> DateTime.now)
        TwittProcessor.RTed += obj

      }
    }

  }
}

object RequeueActor {
  case object Requeue
  case object RequeueComplete
}

class RequeueActor extends Actor with LazyLogging {

  def receive = {
    case RequeueActor.Requeue => {
      val q = MongoDBObject("processed" -> false)
      logger.info(s"Got ${RemoteServices.mongoColl.find(q).count} unprocessed status.")

      val processor = context.actorOf(Props[TwittProcessor], "processor")
      for { st <- RemoteServices.mongoColl.find(q)} {
        st.getAs[Number]("source", "user", "id") match {
          case Some(luid) => {
            val userId = luid.longValue
            val friends_count = RemoteServices.users.findOne(MongoDBObject("_id" -> userId)) match {
              case Some(u) => u.getAs[Number]("friends_count") match {
                case Some(fc) => fc.longValue
                case None => 0
              }
              case None => 0
            }
            if (friends_count > 600) {
              try {
                var tweetId = st.getAs[Number]("source", "id").get.longValue
                processor ! TwittProcessor.FollowAndRT(userId, tweetId)
              } catch {
                case e: Throwable => {
                  println(s"Unable to find user id or source id in tweet: $st")
                }
              }
            }
          }
          case None => { // Ignore source without used id
          }
        }
      }
      sender() ! RequeueActor.RequeueComplete
    }
  }
}

object LanguageActor {
  case object Process
  case object ProcessComplete
}

class LanguageActor extends Actor with LazyLogging {
  import com.cybozu.labs.langdetect.DetectorFactory

  def receive = {
    case LanguageActor.Process => {
      val q = "language" $exists false

      logger.info(s"Got ${RemoteServices.mongoColl.find(q).count} unprocessed status.")

      val processor = context.actorOf(Props[TwittProcessor], "processor")
      for { st <- RemoteServices.mongoColl.find(q)} {
        val detector = DetectorFactory.create
        st.getAs[String]("source", "text") match {
          case Some(text) => {
            detector.append(text)
            val lang = detector.detect
            // println(s"$lang ||| $text")
          }
          case None => {
            println(s"No text here: $st")
          }
        }
      }
      sender() ! LanguageActor.ProcessComplete
    }
  }
}

class Manager extends Actor with LazyLogging {

  override def preStart(): Unit = {
    val harvester = context.actorOf(Props[HarvesterActor], "harvester")
    val languager = context.actorOf(Props[LanguageActor])
    Scheduler.every(harvester ! HarvesterActor.Harvest, 10 * 60 * 1000)
    // val requeuer = context.actorOf(Props[RequeueActor], "requeuer")
    // requeuer ! RequeueActor.Requeue
    // languager ! LanguageActor.Process
  }

  def receive = {
    case msg => logger.info(s"Received message $msg")
    // when the greeter is done, stop this actor and with it the application
    // case Greeter.Done => context.stop(self)
  }
}

object Scheduler {
  import java.util.concurrent.Executors
  import scala.compat.Platform
  import java.util.concurrent.TimeUnit
  private lazy val sched = Executors.newSingleThreadScheduledExecutor();
  def schedule(f: => Unit, time: Long) {
    sched.schedule(new Runnable {
      def run = f
    }, time , TimeUnit.MILLISECONDS);
  }
  def every(f: => Unit, period: Long) {
    sched.scheduleAtFixedRate(new Runnable {
      def run = f
      }, 0, period, TimeUnit.MILLISECONDS)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    import com.cybozu.labs.langdetect.DetectorFactory
    println("Loading profiles")
    DetectorFactory.loadProfile("./profiles")
    println("Loaded profiles")


    akka.Main.main(Array(classOf[Manager].getName))
  }

}

