package app.ecom.dmp.search

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.scalalogging.LazyLogging
import org.json4s._
import org.json4s.native.JsonMethods._

import scalaj.http._

object HttpClient extends LazyLogging {
  val dmpWebHost = "http://innerapi.cct.baidu.com/api/group/groupResult"
  val dmeeWebHost = "http://yq01-ods-spark01.yq01.baidu.com:8855/CctTaskService/CreateTask"
  val dmeeCallBackHost = "http://cq02-dan-gd-core02.cq02.baidu.com:8010/MarketingCloudService/MCRpcCallBackTask"
  val metaHost = "metastore.baidu.com"
  val host = "yq01-dmp-spark-master.yq01:8000"
  val GetOneInstanceToSearchRequest: HttpRequest = Http(s"http://${host}/api/v1/sql-instance/get-one-instance-to-search")
  val PostInstanceResultRequest: HttpRequest = Http(s"http://${host}/api/v1/sql-instance/set-count-for-instance")
    .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
  val format = new SimpleDateFormat("yyyyMMdd")


  //得到需要运行的任务
  def getSQLInstanceNewList(): List[SQLInstance] = {
    val calendar = Calendar.getInstance()
    val start_time = format.format(calendar.getTime)
    try {
      val sqlInstanceRestRequest: HttpRequest = Http(s"http://${host}/api/v1/sql-instance-resource").timeout(connTimeoutMs=10000, readTimeoutMs=50000)
      val response = sqlInstanceRestRequest.param("status", "new").asString
//            val sqlInstanceRestRequest: HttpRequest = Http(s"http://${host}/api/v1/sql-instance-cjj").timeout(connTimeoutMs=10000, readTimeoutMs=50000)
//          val response = sqlInstanceRestRequest.param("status", "copied").param("table","dmp_url_table").param("yymmdd","20180522").asString
      response.code match {
        case 200 =>
          for {
            JObject(instanceList) <- parse(response.body)
            JField("data", JArray(data)) <- instanceList
            JObject(instance) <- data
            JField("id", JInt(id)) <- instance
            JField("save_path", JString(save_path)) <- instance
            JField("yymmdd", JString(yymmdd)) <- instance
            JField("sql_job", JObject(sql_job)) <- instance
            JField("sql_string", JString(sql_string)) <- sql_job
            JField("business_name", JString(business_name)) <- sql_job
            JField("business_id", JInt(business_id)) <- sql_job
          } yield SQLInstance(id.longValue(), sql_string, save_path, yymmdd, business_name, business_id.toString())

        case _ =>
          List.empty
      }

    } catch {
      case e: Exception =>
        logger.info(e.toString)
        List.empty
    }
  }

  def setSQLInstanceStatus(id: Long, currentStatus: Option[String], modifyStatus: Option[String],
                           count: Option[Long], message: Option[String]): Boolean = synchronized {
    try {
      Utils.retry(3) {
        val sqlInstanceRestRequest: HttpRequest = Http(s"http://${host}/api/v1/sql-instance-resource/$id")
          .timeout(connTimeoutMs=10000, readTimeoutMs=50000)
        val param = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
        //logger.info("set instance status ")
        //方法欺骗 库只支持PUT POST请求
        param.append(("_method", "PUT"))
        currentStatus match {
          case Some(status) => param.append(("current_status", status))
          case None =>
        }
        modifyStatus match {
          case Some(status) => param.append(("modify_status", status))
          case None =>
        }
        count match {
          case Some(value) => param.append(("count", value.toString))
          case None =>
        }
        message match {
          case Some(m) => param.append(("message", m))
          case None =>
        }
        val response = sqlInstanceRestRequest.postForm(param.toSeq).asString
        response.code match {
          case 200 => true
          case _ => false
        }
      }
    } catch {
      case e: Exception =>
        logger.info(e.toString)
        false
    }
  }

  def getTableVersion(table: String): Option[String] = synchronized {
    val GetTableVersionRequest: HttpRequest = Http(s"http://${metaHost}/api/dmp")
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
    val response = GetTableVersionRequest.param("table_name", table).param("format", "json").asString
    response.code match {
      case 200 =>
        Some(response.body)
        val body = parse(response.body)
        implicit val formats = DefaultFormats
        try {
          val tableVersion = body.extract[TableVersionRes]
          Some(tableVersion.data)
        } catch {
          case e: Exception =>
            logger.info(e.toString)
            None
        }
      case _ =>
        None
    }
  }

  def postSQLResultToWeb(dmpid: Long, dayToWrite: String, status: String, count: Long): Boolean = {
    var postParam = ""
    if (status == "0") {
      postParam = s"""{"groupId":"$dmpid","cookieDate":"$dayToWrite","uuidNum":"$count","status":"0"}"""
    } else {
      postParam = s"""{"groupId":"$dmpid","cookieDate":"$dayToWrite","uuidNum":"-1","status":"1"}"""
    }
    try {
      val response = Http(dmpWebHost).postData(postParam)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000)).asString
      response.code match {
        case 200 =>
          true
        case _ =>
          false
      }
    } catch {
      case e: Exception =>
        println(e)
        false
    }
  }

  def postSQLResultToDMEE(dmpid: Long, businessName: String, dayToWrite: String, status: String, count: Long): Boolean = {

    var postParam = ""
    if (status == "0") {
      postParam = s"""{"cct_user":{"user_id":0, "user_name": "SQL_SERVICE"}, "task_type": 11, "join_info":{"business_id": $dmpid, "version": $dayToWrite, "num": $count, "business_name":     "$businessName", "status": $status}, "need_callback": true, "callback_addr": "$dmeeCallBackHost", "force_restart":  true}"""
    } else {
      postParam = s"""{"cct_user":{"user_id":0, "user_name": "SQL_SERVICE"}, "task_type": 11, "join_info":{"business_id": $dmpid, "version": $dayToWrite, "num": 0, "business_name":          "$businessName", "status": -1}, "need_callback": true, "callback_addr": "$dmeeCallBackHost", "force_restart":  true}"""
    }
    try {
      val response = Http(dmeeWebHost).postData(postParam)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000)).asString
      response.code match {
        case 200 =>
          true
        case _ =>
          false
      }
    } catch {
      case e: Exception =>
        println(e)
        false
    }
  }

  def log(request: HttpRequest, response: HttpResponse[String]) = {
    //这里用于记录每一个和SERVER交互的LOG信息
    println(request.url)
    println(response.code)
  }

  // 得到开始结束时间的daily表未完成的时间列表
  def getTableDailyNotDone(dmp_id: String, start_yymmdd: String, end_yymmdd: String): Option[String] = {
    //logger.info("httpclient.getTableDailyNotDone start")
    val request: HttpRequest = Http(s"http://metastore.baidu.com/api/check_daily").timeout(connTimeoutMs=10000, readTimeoutMs=50000)
    val response = request.param("dmp_id", dmp_id)
      .param("start_time", start_yymmdd)
      .param("end_time", end_yymmdd)
      .param("format", "json").asString
    response.code match {
      case 200 =>
        val body = parse(response.body)
        implicit val formats = DefaultFormats
        try {
          val tableVersion = body.extract[MessageBody]
          Some(tableVersion.data)
        } catch {
          case e: Exception =>
            logger.info(e.toString)
            None
        }
      case _ =>
        logger.info("getTableDailyNotDone http return code : " + response.code.toString)
        None
    }
  }

  def getTableSrcDone(table_name: String, daily_not_done: String): Option[String] = {
    val request: HttpRequest = Http(s"http://metastore.baidu.com/api/check_src_table")
      .timeout(connTimeoutMs=10000, readTimeoutMs=50000)
    val response = request.param("table_name", table_name)
      .param("date_range", daily_not_done)
      .param("format", "json").asString
    response.code match {
      case 200 =>
        val body = parse(response.body)
        implicit val formats = DefaultFormats
        try {
          val tableVersion = body.extract[MessageBody]
          Some(tableVersion.data)
        } catch {
          case e: Exception =>
            logger.info(e.toString)
            None
        }
      case _ =>
        None
    }
  }

  def setTableDailyStatus(dmp_id: String, day: String, status: String): Boolean = synchronized {
    val postParam = s"""{"dmp_id": "$dmp_id", "version":"$day", "status":"$status"}"""
    Utils.retry(3) {
      try {
        val response = Http(s"http://metastore.baidu.com/api/update_daily").postData(postParam)
          .header("Content-Type", "application/json")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(10000)).asString
        response.code match {
          case 200 =>
            true
          case _ =>
            false
        }
      } catch {
        case e: Exception =>
          println(e)
          false
      }
    }
  }
}