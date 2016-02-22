package org.zoy.oct
import twitter4j._
import twitter4j.conf.ConfigurationBuilder
import scala.collection.JavaConverters._
import com.mongodb.casbah.Imports._
import twitter4j.json.DataObjectFactory

import com.mongodb.util.JSON
import com.mongodb.DBObject

object RemoteServices {

  val mongoConn = MongoConnection()
  val mongoDB = mongoConn("ouiouistiti")
  var users = mongoDB("users")
  val mongoColl = mongoConn("ouiouistiti")("tweets")
  val creds = scala.io.Source.fromFile("creds.txt").mkString.split("\n")
  val searches = creds(4).split("\\|")
  val banned = creds(5).split("\\|").map(_.toLong)

  val twitter = {
    val cb = new ConfigurationBuilder()
    val Array(ckey, csecret, at, ats) = creds.slice(0,4)
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(ckey)
      .setOAuthConsumerSecret(csecret)
      .setOAuthAccessToken(at)
      .setOAuthAccessTokenSecret(ats)
      .setJSONStoreEnabled(true)

    val tf = new TwitterFactory(cb.build())
    tf.getInstance()
  }
}
