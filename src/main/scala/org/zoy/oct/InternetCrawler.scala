package org.zoy.oct

import akka.actor.Actor
import akka.actor.Props
import com.typesafe.scalalogging.LazyLogging
import de.jetwick.snacktory.ArticleTextExtractor
import scalaj.http._
import com.mongodb.casbah.Imports._
import com.cybozu.labs.langdetect.DetectorFactory

import net.ruippeixotog.scalascraper.browser.Browser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL.Parse._
import com.cybozu.labs.langdetect.LangDetectException
import java.net.URI

import edu.stanford.nlp.tagger.maxent._
import edu.stanford.nlp.io.IOUtils
import java.io.StringReader

object InternetCrawler {
  def cache = {
    val cache = RemoteServices.mongoConn("ouiouistiti")("cache")
    cache.createIndex(MongoDBObject("url" -> 1))
    cache.createIndex(MongoDBObject("lang" -> 1))
    cache
  }
  def browser = new Browser
  def tagger = {
    val tagger = new MaxentTagger("french.tagger")
    tagger
  }

  case class Crawl(sourceUrl: String, depth: Int)
  case class PickADoc(lang:String)
  case object AssignLanguage
  case object WorkComplete
}

class InternetCrawler extends Actor with LazyLogging {
  def receive = {
    case InternetCrawler.Crawl(source, depth) => {
      crawl(source, depth)
      sender() ! InternetCrawler.WorkComplete
    }
    case InternetCrawler.AssignLanguage => {
      assignLanguage
      sender() ! InternetCrawler.WorkComplete
    }
    case InternetCrawler.PickADoc(lang) => {
      pickADoc(lang)
      sender() ! InternetCrawler.WorkComplete
    }
  }

  def pickADoc(lang: String) = {

    InternetCrawler.cache.find("lang" $eq lang).limit(1).foreach{ doc =>
      val body = doc.getAs[String]("body").get
      val ae = new ArticleTextExtractor
      val res = ae.extractContent(body)

      // println(s"Title: ${res.getText}")
      val sr = new StringReader(res.getText)

      val sentences = MaxentTagger.tokenizeText(sr)
      val ts = InternetCrawler.tagger.tagSentence(sentences(0))
      println(s"${ts}")
      // println(InternetCrawler.tagger.tagString(res.getText))
      // println(s"${res.getImageUrl}")
    }
  }

  def assignLanguage() = {
    InternetCrawler.cache.find( $and(("lang" $exists false) :: ("invalid" $exists false))).foreach{ doc =>
      logger.info(s"Analyzing language for ${doc._id.get}")
      val body = doc.getAs[String]("body").get
      val ae = new ArticleTextExtractor
      if (body.length > 10) {
        val res = ae.extractContent(body)
        try {
        val detector = DetectorFactory.create
        detector.append(res.getText)
        val lang = detector.detect
        doc.put("lang", lang)
        InternetCrawler.cache += doc
        println(s"$lang")
        } catch {
          case e:LangDetectException => {
            InternetCrawler.cache -= doc
          }
        }
      } else {
        logger.info(s"Discarding ${doc._id}")
        InternetCrawler.cache -= doc
      }
    }
  }

  def crawl(sourceUrl: String, depth: Int) = {
    if (depth > 0) {

      val uri = new URI(sourceUrl)


      val document = InternetCrawler.cache.findOne(MongoDBObject("url" -> sourceUrl)) match {
        case Some(data) => {}
        case None => {
          logger.info(s"$depth - Miss in cache for $sourceUrl")
          try {
            val response: HttpResponse[String] = Http(sourceUrl).asString
            val document = MongoDBObject("url" -> sourceUrl,
              "body" -> response.body)

            // Inject in mongo
            InternetCrawler.cache += document

            // Extract links and enqueue

            val body = document.getAs[String]("body").get
            val doc = InternetCrawler.browser.parseString(body)
            var items = doc >> elements("a")
            var links = items.map(_ .attr("href")).
            filter{ targetUri =>
              targetUri != "#" && targetUri != ""
            }.
            map{ targetUri =>
              println(targetUri)
              if (targetUri.contains("#")) targetUri.split("#")(0) else targetUri
            }.
            map{ targetUri =>
              if (targetUri.startsWith("/")) {
                uri.resolve(targetUri).toString
              } else {
                targetUri
              }
            }.foreach{ url =>
              val crawler = context.actorOf(Props[InternetCrawler])
              crawler ! InternetCrawler.Crawl(url, depth - 1)
            }
          } catch {
            case e: java.net.MalformedURLException => {}
            case f: Throwable => logger.error(s"Error while loading $sourceUrl:\n$f")
          }
        }
      }
    }
  }
}


class CrawlerManager extends Actor with LazyLogging {

  override def preStart(): Unit = {
    val crawler = context.actorOf(Props[InternetCrawler])

    crawler ! InternetCrawler.Crawl("https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Accueil_principal", 8)
    //crawler ! InternetCrawler.AssignLanguage
    //crawler ! InternetCrawler.PickADoc("fr")
  }

  def receive = {
    //case InternetCrawler.WorkComplete => context.stop(self)
    case msg => logger.info(s"Received message $msg")
  }
}

object CrawlerMain {
  def main(args: Array[String]): Unit = {
    import com.cybozu.labs.langdetect.DetectorFactory
    println("Loading profiles")
    DetectorFactory.loadProfile("./profiles")
    println("Loaded profiles")


    akka.Main.main(Array(classOf[CrawlerManager].getName))
  }

}
