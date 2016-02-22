package org.zoy.oct

import akka.actor.Actor
import akka.actor.Props
import com.typesafe.scalalogging.LazyLogging
import twitter4j._
import scala.collection.JavaConverters._
import com.mongodb.util.JSON
import com.mongodb.casbah.Imports._
import twitter4j.json.DataObjectFactory

object HarvesterActor {
  case object Harvest
  case object WorkComplete
}

class HarvesterActor extends Actor with LazyLogging {
  def receive = {
    case HarvesterActor.Harvest => {
      harvest
      sender() ! HarvesterActor.WorkComplete
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
      logger.info(s"Searching for $currentSearch")
      val query = new Query(currentSearch)
      query.setResultType(Query.ResultType.recent)
      val result = RemoteServices.twitter.search(query)

      val tweets = result.getTweets.asScala
      tweets.map{ tweet =>
        saveOrIgnore(currentSearch, firstTweet(tweet))
      }
    }

    val requeuer = context.actorOf(Props[RequeueActor])
    requeuer ! RequeueActor.Requeue
  }
}
