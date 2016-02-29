package org.zoy.oct

import akka.actor.Actor
import akka.actor.Props
import scala.concurrent.Future
import akka.pattern.CircuitBreaker

import com.typesafe.scalalogging.LazyLogging
import twitter4j._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.mongodb.util.JSON
import com.mongodb.casbah.Imports._

import twitter4j.json.DataObjectFactory
import twitter4j.{Status, QueryResult, TwitterException}
import com.github.nscala_time.time.Imports.DateTime


object TwittProcessor {

  case class FollowAndRT(userId: Long, statusId: Long)
  case class SearchFor(query: String)
  case object StoreFollowers

  val following = RemoteServices.mongoConn("ouiouistiti")("following")
  val RTed = RemoteServices.mongoConn("ouiouistiti")("retweeted")
}

import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers


class TwittProcessor extends Actor with LazyLogging {
  RegisterJodaTimeConversionHelpers.register()
  import context.dispatcher

  val READONLY = true

  def ReadOnlyFilter(f: => Unit ) = {
    if (!READONLY) {
      f
    }
  }

  val breaker =
    new CircuitBreaker(context.system.scheduler,
      maxFailures = 2,
      callTimeout = 10.seconds,
      resetTimeout = 50.minute).onOpen(notifyMeOnOpen())

  def notifyMeOnOpen(): Unit =
    logger.warn("Twitter has reach Circuit Breaker threshold.")


  def getOrLogAndThrow[T](f: => T): T = try {
    f
  } catch {
    case e: Throwable => {
      logger.error(s"Got throwable $e")
      e.printStackTrace
      throw e
    }
  }

  def receive = {
    case TwittProcessor.SearchFor(search: String) => {
      val harvester = context.actorOf(Props[HarvesterActor], "resultStore")
      harvester ! breaker.withSyncCircuitBreaker(performSearch(search))
    }
    case TwittProcessor.FollowAndRT(userId, statusId) => {
     val user = TwittProcessor.following.findOne(MongoDBObject("_id" -> userId))
      user match {
        case Some(u) => logger.info(s"Already following $userId")
        case None => breaker.withSyncCircuitBreaker(followUser(userId))
      }
      breaker.withCircuitBreaker(Future(retweet(statusId)))
    }
  }

  def performSearch(currentSearch: String) = {
    logger.info(s"Searching for $currentSearch")
    val query = new Query(currentSearch)
    query.setResultType(Query.ResultType.recent)
    val result = getOrLogAndThrow(RemoteServices.twitter.search(query))

    HarvesterActor.StoreResult(currentSearch, result)
  }
  def RTStatus(statusId: Long) = {
    try { RemoteServices.twitter.tweets.retweetStatus(statusId) }
    catch {
      case e: TwitterException =>
        if (e.resourceNotFound) {
          logger.info(s"Status $statusId not found.")
        } else {
          throw e
        }
      case e: Throwable => throw e
    }
  }
  // RW methods
  def followUser(userId: Long) = ReadOnlyFilter {
    if (TwittProcessor.following.count() > 1000) {
       val extras = TwittProcessor.following.find(MongoDBObject.empty)
        .sort(orderBy = MongoDBObject("followDate" -> 1))
        .skip(0).limit(10).toList
      extras.map{ doc =>
        getOrLogAndThrow(RemoteServices.twitter.friendsFollowers.destroyFriendship(doc._id.get.toString.toLong))
      }
    }

    logger.info(s"Following $userId")
    getOrLogAndThrow(RemoteServices.twitter.friendsFollowers.createFriendship(userId))
    val obj = MongoDBObject("_id" -> userId, "followDate" -> DateTime.now)
    TwittProcessor.following += obj
  }
  def retweet(statusId: Long) = ReadOnlyFilter {
    if (TwittProcessor.RTed.find(MongoDBObject("_id" -> statusId)).count == 1) {
      logger.info(s"Already Retweeted $statusId")
    } else {
      logger.info(s"RTing $statusId")
      // 404
      getOrLogAndThrow(RTStatus(statusId))
      val obj = MongoDBObject("_id" -> statusId, "rtDate" -> DateTime.now)
      TwittProcessor.RTed += obj

    }
  }
}

object HarvesterActor {
  case object Harvest
  case object WorkComplete
  case class StoreResult(currentSearch: String, results: QueryResult)
}


class HarvesterActor extends Actor with LazyLogging {
  def receive = {
    case HarvesterActor.Harvest => {
      val tw = context.actorOf(Props[TwittProcessor])
      harvest
    }
    case HarvesterActor.StoreResult(search, results) => {
      logger.info(s"Storing results for $search")
      results.getTweets.asScala.map{ tweet =>
        saveOrIgnore(search, firstTweet(tweet))
      }

      val requeuer = context.actorOf(Props[RequeueActor])
      requeuer ! RequeueActor.Requeue
    }
  }

  def saveOrIgnore(currentSearch: String, status: Status) = {
    import scala.util.matching.Regex
    JSON.parse(DataObjectFactory.getRawJSON(status)).asInstanceOf[DBObject].getAs[DBObject]("user") match {
      case Some(u) => {
        val user = u ++ MongoDBObject("_id" -> status.getUser.getId)
        val users = RemoteServices.users
        users += user
      }
      case None => {
        logger.error(s"Unable to get user in status $status")
      }
    }


    if (RemoteServices.banned.contains(status.getUser.getId)) {
      logger.info(s"Ignoring banned user ${status.getUser.getScreenName}.")
    } else {
      val txt = status.getText
      val valid = currentSearch.split(" ").forall(kwd => txt.toLowerCase.contains(kwd.toLowerCase) || kwd.startsWith("-"))
      val valid2 = "\\Wrt\\W".r.findFirstIn(txt.toLowerCase)
      if (!valid || valid2 == None) {
        logger.info(s"Rejecting status, missing words ($currentSearch): $txt")
      } else {
        val id = status.getId
        val q = MongoDBObject("_id" -> id)
        val count = RemoteServices.mongoColl.find(q).count
        if (count == 0) {
          val stringStatus: String = DataObjectFactory.getRawJSON(status);
          val obj = MongoDBObject("_id" -> id,
            "source" ->  JSON.parse(stringStatus).asInstanceOf[DBObject],
            "processed" -> false
            )
          RemoteServices.mongoColl += obj
          logger.info(s"Adding status $id to database.")
        } else {
          logger.info(s"Ignoring status $id already seen before.")
        }
      }
    }
  }

  def firstTweet(someTweet: Status): Status = {
    if (someTweet.isRetweet) {
      val status = RemoteServices.twitter.showStatus(someTweet.getRetweetedStatus.getId)
      return firstTweet(status)
    } else {
      return someTweet
    }
  }

  def harvest = {
    RemoteServices.searches.foreach{ currentSearch =>
      val tw = context.actorOf(Props[TwittProcessor])
      tw ! TwittProcessor.SearchFor(currentSearch)
    }
  }
}
