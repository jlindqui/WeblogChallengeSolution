import java.util.Date

case class AccessLogEvent(timestamp: Date, clientPort: String, requestUrl: String)

object AccessLogEvent extends Enumeration {
  val TIMESTAMP = 0
  val ELB = 1
  val CLIENT_PORT = 2
  val REQUEST_URL = 12 //Note - While this works in the case we're doing, and is slightly more efficient
  //because we don't need to do any regex work, a solution that would be more robust would break it down by this regex:
  //to ensure that there can be spaces if they are in quotes: ([^\"]\\S*|\".+?\")\\s*
}
