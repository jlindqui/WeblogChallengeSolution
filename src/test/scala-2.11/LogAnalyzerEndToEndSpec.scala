import org.apache.spark.rdd.RDD

class LogAnalyzerEndToEndSpec extends SparkBaseSpec {

  lazy val testFile: RDD[String] = testContext.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

  "log analyzer " should "determine a variety of statistics" in {

    val sessionizedTestFile: RDD[(String, List[Session])] = LogAnalyzer.sessionizeByIp(testFile)
    val sessionList = sessionizedTestFile.flatMap(_._2)

    //Determine the average session time
    val averageSessionTime = LogAnalyzer.averageSessionTime(sessionList)

    averageSessionTime shouldBe 14487.383547154768D //Average about 9.73 minutes

    //Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    val topClicks = sessionList.map(_.uniqueClicks).top(3)
    topClicks(0) shouldBe 411
    topClicks(1) shouldBe 322
    topClicks(2) shouldBe 292

    //Find the most engaged users, ie the IPs with the longest session times
    val mostEngaged = LogAnalyzer.findMostEngagedUsers(sessionizedTestFile, 3)

    mostEngaged(0) shouldBe "213.239.204.204:35094"
    mostEngaged(1) shouldBe "103.29.159.138:57045"
    mostEngaged(2) shouldBe "78.46.60.71:58504"
  }
}
