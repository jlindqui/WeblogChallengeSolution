import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

abstract class SparkBaseSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  protected var testContext: SparkContext = null

  override protected def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("App Name").setMaster("local")
    testContext = new SparkContext(conf)
  }

  override protected def afterAll(): Unit = {
    testContext.stop()
  }

}
