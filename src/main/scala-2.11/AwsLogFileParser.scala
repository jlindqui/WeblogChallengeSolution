import javax.xml.bind.DatatypeConverter

object AwsLogFileParser {
  def parseEvent(s: String): AccessLogEvent = {
    val split = s.split(" ")
    AccessLogEvent(DatatypeConverter.parseDateTime(split(AccessLogEvent.TIMESTAMP)).getTime, split(AccessLogEvent.CLIENT_PORT), split(AccessLogEvent.REQUEST_URL))
  }
}
