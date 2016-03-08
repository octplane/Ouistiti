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
  val mongoColl = mongoDB("tweets")

  val creds = scala.io.Source.fromFile("creds.txt").mkString.split("\n")
  val searches = creds(0).split("\\|")
  val banned = creds(1).split("\\|").map(_.toLong)

  val twitter = {
    val tf = new TwitterFactory("twitter4j.conf")
    tf.getInstance()
  }
}
