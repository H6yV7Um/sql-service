package app.ecom.dmp.search

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import scala.collection.mutable.Map
import scala.collection.mutable.SynchronizedMap
import scala.collection.mutable.HashMap
import scala.util.Try

object Utils {

  //保存表名和路径的映射
  val tables = new HashMap[String, String] with
    SynchronizedMap[String, String]
  ()

  //对一个函数重试N次
  def retry[T](n: Int)(fn: => T): T = {
    util.Try {
      fn
    } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }

  //
  def nDaysAgo(longestDay: Int): String = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    for (x <- 0 until longestDay) {
      calendar.add(Calendar.DAY_OF_YEAR, -1)
    }
    format.format(calendar.getTime())
  }

  //更新当前数据源的基准
  def updateTableVersion(tableName: String, version: String): Unit = {
    tables += (tableName -> version)
  }

  //获取数据表的最新基准
  def getTableVersion(tableName: String): Option[String] = {
    Try {
      tables(tableName)
    }.toOption
  }

  def getAllTables(): HashMap[String, String] = {
    return tables
  }

}
