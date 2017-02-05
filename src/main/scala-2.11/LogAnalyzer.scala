import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration

object LogAnalyzer {

  /**
    * Group a list of events by duration. Events are grouped if their timestamps are less than the given duration.
    * This does not take into account user IPs, so if they need to be separated by IP that would need to be done before.
    *
    * @param duration The maximum duration of time to consider 2 events part of the same session
    * @param events   An iterable list of AccessLogEvents.
    * @return A list of sessions
    */
  def sessionize(duration: Duration)(events: Iterable[AccessLogEvent]): List[Session] = {

    //This will look at each element and keep track of the last previous time. If the duration is too much,
    //create a new sequence and add it to the list. Otherwise, add it to the last sequence.
    //Note: This will end with sequences out of order.
    def loop(events: List[AccessLogEvent], prevTime: Date, splitEvents: List[List[AccessLogEvent]]): List[List[AccessLogEvent]] = {
      events match {
        case Nil => splitEvents
        case h :: t if !timeExpired(TimeWindow(prevTime, h.timestamp), duration) => loop(t, h.timestamp, (h :: splitEvents.head) :: splitEvents.tail)
        case h :: t => loop(t, h.timestamp, List(h) :: splitEvents)
      }
    }

    //Sort each list of events by timestamp
    loop(events.toList.sortBy(_.timestamp), events.head.timestamp, List(List.empty[AccessLogEvent]))
      .filter(_.nonEmpty)
      .map {
        case eventsPerSession =>
          val duration =
            if (eventsPerSession.size == 1)
              0
            else
              Math.abs(eventsPerSession.head.timestamp.getTime - eventsPerSession.last.timestamp.getTime)

          Session(eventsPerSession, events.head.clientPort, duration, uniqueHits(eventsPerSession))
      }
  }

  def isWithinTime(timeWindow: TimeWindow)(event: AccessLogEvent): Boolean =
    timeWindow.startTime.before(event.timestamp) && timeWindow.endTime.after(event.timestamp)

  //Sessionize the web log by IP. Sessionize = aggregate all page hits by visitor/IP during a fixed time window.
  def sessionizeByIp(data: RDD[String], timeWindow: Option[TimeWindow] = None, duration: Duration = Duration(15, TimeUnit.MINUTES)): RDD[(String, List[Session])] = {
    data
      .map(AwsLogFileParser.parseEvent)
      .filter(event => timeWindow match {
        case None => true
        case Some(window) => isWithinTime(window)(event)
      })
      .groupBy(_.clientPort)
      .mapValues(sessionize(duration))
  }

  //Determine the average session time
  //Note - This is counting a 0 second hit. It could also make sense to remove 0 second sessions, to see the average session time when there is activity.
  def averageSessionTime(data: RDD[Session]) = data.map(_.duration).mean()

  def averageSessionTime(sessions: List[Session]): Long = {
    val sum = sessions.map(_.duration).sum
    sum / sessions.size
  }

  //Note: I counted them as distinct if the whole path including query parameters were the same. It may be of interest
  //to consider www.url.com?q=1 as the same as www.url.com?q=2, but I counted them separately.
  def uniqueHits(events: List[AccessLogEvent]) =
    events.map(_.requestUrl).distinct.size

  //Return true if the difference between the start and end time is more than the provided duration
  def timeExpired(timeWindow: TimeWindow, duration: Duration): Boolean = Math.abs(timeWindow.endTime.getTime - timeWindow.startTime.getTime) > duration.toMillis

  /**
    * Determine a list of the users with the longest average duration.
    *
    * @param data        A tuple of String identfifiers and a list of sessions.
    * @param numToReturn The number of users to return.
    * @return An array of maximum numToReturn identifiers that correspond to the largest sessions.
    */
  def findMostEngagedUsers(data: RDD[(String, List[Session])], numToReturn: Int = 10): Array[String] =
    data
      .map { case (ip, sessions) =>
        (ip, averageSessionTime(sessions))
      }
      .top(numToReturn)(Ordering.by(_._2))
      .map(_._1)

}
