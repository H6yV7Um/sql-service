package app.ecom.dmp.search

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._
import scala.util.Try

object SparkInstance {
  def getSQLContext(): SparkSession = {
    SparkSession
      .builder()
      .appName("sql-service-v3")
      .getOrCreate()
  }

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("sql-service-v3")
      .getOrCreate()
  }
}

case class Person(target_id: Long, baiduid: String)

object Main {
  def main(args: Array[String]) {
    val props = new Properties
    props.load(getClass().getResourceAsStream("/mylog4j.properties"))
    PropertyConfigurator.configure(props)
    val conf = ConfigFactory.load("actor")
    val spark = SparkInstance.getSparkSession
    //初始化actor system
    val system = ActorSystem("sql-service", conf)
    init


    val tableManagerActor = system.actorOf(Props[TableManagerActor], name = "TableManagerActor")
    val sqlMasterActor = system.actorOf(Props[SqlMasterActor], name = "SqlMasterActor")
    val refreshTableTimeActor = system.actorOf(Props[RefreshTableTimeActor], name = "RefreshTableTimeActor")
    import system.dispatcher
    system.scheduler.schedule( 600 seconds, 130 seconds, sqlMasterActor, "schedule")
    system.scheduler.schedule(20 seconds, 1800 seconds, tableManagerActor, "refreshTable")
    system.scheduler.schedule(20 seconds, 1800 seconds, refreshTableTimeActor, "updateTableTime")
    system.awaitTermination()
    system.shutdown()
    spark.stop
  }

  def init: Unit = {
    //TimeZone
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))
    //register udf
    SparkInstance.getSQLContext.udf.register("recent_day", (day: Int, now: String) => {
      val format = new SimpleDateFormat("yyyyMMdd")
      val current = format.parse(now)
      val calendar = Calendar.getInstance()
      calendar.setTime(current)
      for (x <- 0 until day) {
        calendar.add(Calendar.DAY_OF_YEAR, -1)
      }
      format.format(calendar.getTime()).toInt
    })
    //register udf
    SparkInstance.getSQLContext.udf.register("days_ago_by_instance", (day: Int, now: String) =>
      synchronized {
        var date: String = ""
        val format = new SimpleDateFormat("yyyyMMdd")
        val table = now
        Utils.getTableVersion(table) match {
          case Some(version) =>
            date = version
          case None =>
            val cal: Calendar = Calendar.getInstance()
            date = format.format(cal.getTime())
        }
        val current = format.parse(date)
        val calendar = Calendar.getInstance()
        calendar.setTime(current)
        for (x <- 0 until day) {
          calendar.add(Calendar.DAY_OF_YEAR, -1)
        }
        format.format(calendar.getTime()).toInt
      })


    SparkInstance.getSQLContext.udf.register("days_ago_by_source", (day: Int, sourceName: String) => {
      HttpClient.getTableVersion(sourceName) match {
        case Some(version) =>
          val format = new SimpleDateFormat("yyyyMMdd")
          val current = format.parse(version)

          val calendar = Calendar.getInstance()
          calendar.setTime(current)
          for (x <- 0 until day) {
            calendar.add(Calendar.DAY_OF_YEAR, -1)
          }
          format.format(calendar.getTime()).toInt
        case None =>
          0
      }
    })

    def is_match_url(regs: String, url: String): Boolean = {
      if (url == null || "".equals(url)) return false;
      if (regs == null || "".equals(regs)) return false;
      val arr = regs.split("\01");
      if (arr != null && arr.length > 0) {
        arr.foreach(reg => {
          val index = url.indexOf(reg)
          if (index == -1) {
            return false
          } else {
            return true
          }
        })
      }
      return false;
    }

    SparkInstance.getSQLContext.udf.register("is_match_url", is_match_url _)
    SparkInstance.getSQLContext.udf.register("category_count", DmpUdf.find_id_func _)
    SparkInstance.getSQLContext.udf.register("category_contains", DmpUdf.two_array_contains _)
  }
}

//下面这个是义山提供的
object DmpUdf {
  def find_id_func(arrayCol: Seq[Seq[Long]], valueArray: Seq[Long]): Option[Long] = {
    Try {
      var frequency = 0l
      for (index_value <- (0 until valueArray.length)) {
        for (index_base <- (0 until arrayCol.length)) {
          if ((arrayCol(index_base)).contains(valueArray(index_value))) {
            frequency = frequency + 1
          }
        }
      }
      frequency
    }.toOption
  }

  def two_array_contains(arrayCol: Seq[Seq[Long]], id: Int): Option[Boolean] = {
    Try {
      var find = false
      var index_base = 0
      while ((index_base < arrayCol.length) && !find) {
        if ((arrayCol(index_base)).contains(id)) {
          find = true;
        }
        index_base = index_base + 1
      }
      find
    }.toOption
  }
}
