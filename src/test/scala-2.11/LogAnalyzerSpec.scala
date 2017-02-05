import java.util.{Calendar, Date}
import java.util.concurrent.TimeUnit
import javax.xml.bind.DatatypeConverter

import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration

class LogAnalyzerSpec extends SparkBaseSpec {

  lazy val testFile: RDD[String] = testContext.textFile("data/SampleTestFile.log")

  "log analyzer given a file and a time window " should "produce sessionized data on the time window" in {

    val startTime = DatatypeConverter.parseDateTime("2015-07-22T10:30:00.000000Z").getTime
    val endTime = DatatypeConverter.parseDateTime("2015-07-22T10:33:00.000000Z").getTime

    val sessionized: Array[(String, List[Session])] = LogAnalyzer.sessionizeByIp(testFile, Some(TimeWindow(startTime, endTime))).collect()

    val sessions = sessionized.map(_._2)
    sessions.foreach { eventList =>
      eventList.size shouldBe 1 //Each user should have a session
      eventList.head.events.size shouldBe 1 //Each session should be 1 event
    }
  }

  "log analyzer given a file and no restictions" should "produce sessionized data on the whole file" in {

    val sessionized: Array[(String, List[Session])] = LogAnalyzer.sessionizeByIp(testFile).collect()

    val user1 = sessionized.filter(_._1 == "122.161.113.239:29236")
    val user2 = sessionized.filter(_._1 == "122.161.113.239:29237")

    //Should be grouped by user
    user1.length shouldBe 1
    user2.length shouldBe 1

    val user1Session = user1.head._2
    val user2Sessions: List[Session] = user2.head._2

    user1Session.head.ip shouldBe "122.161.113.239:29236"
    //Should be grouped into sessions with 15 minute expirations
    user1Session.size shouldBe 1
    user2Sessions.size shouldBe 4

    user1Session.head.duration shouldBe 2520000L
    user2Sessions.foreach(_.duration shouldBe 0)

    user1Session.head.uniqueClicks shouldBe 3

  }

  "given a session " should "determine number of unique url visits" in {

    val events = List(AccessLogEvent(Calendar.getInstance().getTime, "1", "www.test.com/1"),
      AccessLogEvent(Calendar.getInstance().getTime, "1", "www.test.com/2"),
      AccessLogEvent(Calendar.getInstance().getTime, "1", "www.test.com/2"),
      AccessLogEvent(Calendar.getInstance().getTime, "1", "www.test.com/1"),
      AccessLogEvent(Calendar.getInstance().getTime, "1", "www.test.com/3"),
      AccessLogEvent(Calendar.getInstance().getTime, "1", "www.test.com/1")
    )

    LogAnalyzer.uniqueHits(events) shouldBe 3
  }


  "given a list of events and a duration" should "split into a list of sessions, split by the duration" in {

    val events = (1 to 10).map(i =>

      //escalating timestamp differences. The differences should be 0, 300, 500, 700, 900,1100, etc. Therefore, the first 5 should be grouped and the rest single sessions.
      //Note the duration is set to 1 minute for this test.
      AccessLogEvent(new Date(i * i * 100), "1", "request")).toList

    val sessions: List[Session] = LogAnalyzer.sessionize(Duration(1, TimeUnit.SECONDS))(events)

    sessions.head.events.size shouldBe 1
    sessions.last.events.size shouldBe 5
  }

}
