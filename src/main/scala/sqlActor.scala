package app.ecom.dmp.search

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, _}


class SqlMasterActor extends Actor with LazyLogging {
  val conf = ConfigFactory.load("actor")
  val system = ActorSystem("sql-service", conf)
  val workerRouter = system.actorOf(Props[SqlWorker].withDispatcher("my-thread-pool-dispatcher").withRouter(akka.routing.RoundRobinRouter(15)), name = "workerRouter")
  //val workerRouter = context.actorOf(Props[SqlWorker].withRouter(akka.routing.RoundRobinRouter(15)), name = "workerRouter")
  val idWaitToRun = scala.collection.mutable.Set.empty[Long]


  var finishedCount = 0
  var scheduleCount = 0

  var index: Int = 0

  def receive = {
    case "schedule" =>
      logger.info("schedule")
      try {
        HttpClient.getSQLInstanceNewList() foreach { instance =>

          idWaitToRun contains instance.id match {
            case false =>
              idWaitToRun.add(instance.id)

              workerRouter ! instance
            case true =>
          }

        }
        scheduleCount = scheduleCount + 1
        if (scheduleCount >= 18) {
          scheduleCount = 0
          logger.info(s"this time done $finishedCount")
          finishedCount = 0
        }
        logger.info(s"doingList ${idWaitToRun}")

      } catch {
        case e: Exception =>
          logger.warn(s"${e}")
      }
    case compeleteInstanceId: Long =>
      logger.info("remove complete id " + compeleteInstanceId)
      logger.info(s"${compeleteInstanceId} add ${index}")
      index = index + 1
      finishedCount = finishedCount + 1
      idWaitToRun remove compeleteInstanceId
      if (idWaitToRun.size < 12) {
        logger.info("self schedule")
        self ! "schedule"
      }
    case _ =>
      logger.info("can`t match any receive")
  }

  override def preStart(): Unit = {

    logger.info("sqlManager start")
  }

  override def postStop(): Unit = {
    logger.info("sqlManager stop")
  }
}

class SqlWorker extends Actor with LazyLogging {
  val prefixSavePath = "/app/ecom/cm/cm_cct/online/cookie_input/fangwu"
  val prefixCrossTablePath = "/app/ecom/cm/cm_cct/sql-result"
  val dmeeDataPath = "/app/ecom/cm/cm_cct/online/cookie_input/dmee"
  val dmeeDataTmpPath = "/app/ecom/cm/cm_cct/online/cookie_input/sqljoin"

  //  val prefixSavePath = "/app/ecom/cm/cm_cct/wangchao/online/cookie_input/fangwu"
  //  val prefixCrossTablePath = "/app/ecom/cm/wangchao/cm_cct/sql-result"
  //  val dmeeDataPath = "/app/ecom/cm/cm_cct/wangchao/online/cookie_input/dmee"
  //  val dmeeDataTmpPath = "/app/ecom/cm/wangchao/cm_cct/online/cookie_input/sqljoin"

  val dailyPath = "/app/ecom/cm/ods/sql-service/daily_totally"
  val format = new SimpleDateFormat("yyyyMMdd")

  def receive = {
    case instance: SQLInstance =>
      if (instance.sql.indexOf("cross_table") < 0) {

        //抢占该任务
        HttpClient.setSQLInstanceStatus(instance.id, Some("new"), Some("searching"), None, Some("")) match {
          //run
          case true =>
            try {
              val regex = """(?s).*FROM (.*?)\s.*""".r
              val count = 0
              val regex(tableName) = instance.sql
              val sql = instance.sql.replaceAll("\\{\\{.*?instance-day.*?\\}\\}", s""""${tableName.trim()}"""")
              logger.info(s"[TIME] [START] [instanceid ${instance.id}] [timestamp] ${instance.business_id}")
              //结果目录如果已经存在，先删除
              val dstPathForProfile = s"$prefixSavePath/yymmdd=${instance.yymmdd}/dmpid=${instance.business_id}"
              val dstPathForCrossCompute = s"$prefixCrossTablePath/dmp_id=${instance.business_id}/yyyymmdd=${instance.yymmdd}"
              val dmeeJoinPath = s"$dmeeDataTmpPath/yymmdd=${instance.yymmdd}/dmpid=${instance.business_id}"
              val dmeePathForProfile = s"$dmeeDataPath/yymmdd=${instance.yymmdd}/dmpid=${instance.business_id}"
              clearPath(dstPathForProfile)
              clearPath(dstPathForCrossCompute)
              //确定结果存储路径
              var dstPath = dstPathForProfile
              if (instance.business_name == "cct-query") {
                dstPath = dstPathForProfile
              } else if (instance.business_name == "dmee-baidu") {
                dstPath = dmeePathForProfile
              } else if (instance.business_name == "dmee-customer") {
                dstPath = dmeeJoinPath
              }
              val multiSql = sql.split(";")
              if (multiSql.size == 1) {
                logger.info("size==1")
                //  logger.info(multiSql(0))
                doSQLJob(tableName,
                  instance,
                  multiSql(0),
                  org.apache.spark.sql.SaveMode.Overwrite,
                  dstPath,
                  dstPathForCrossCompute)
              } else if (multiSql.size > 1) {
                logger.info("size>1")
                for (q <- multiSql) {
                  // logger.info(q)
                  doSQLJob(tableName,
                    instance,
                    q,
                    org.apache.spark.sql.SaveMode.Append,
                    dstPath,
                    dstPathForCrossCompute)
                }
              }
              val endtime = (new Date()).getTime / 1000
              logger.info(s"[TIME] [END] [instanceid ${instance.business_id}] [timestamp ${endtime}]")
            } catch {
              case e: Exception =>
                logger.info(e.toString)
                HttpClient.setSQLInstanceStatus(instance.id, None, Some("error"), None, Some(s"$e"))
                val endtime = (new Date()).getTime / 1000
                logger.error(s"[TIME] [ERR] [instanceid ${instance.business_id}] [timestamp ${endtime}]${e}")
                var i = 0
                while (i < 3) {
                  logger.info(s"${instance.id} start to post result to online")
                  var flag = false
                  if (instance.business_name == "cct-query") {
                    flag = HttpClient.postSQLResultToWeb(instance.business_id.toLong, instance.yymmdd, "1", 0)
                  } else if (instance.business_name == "dmee-baidu" ||
                    instance.business_name == "dmee-customer") {
                    flag = HttpClient.postSQLResultToDMEE(instance.business_id.toLong, instance.business_name, instance.yymmdd, "1", 0)
                  }
                  if (flag == false) {
                    logger.warn(s"${instance.id} call back online api failed")
                    i = i + 1
                    Thread.sleep(10)
                  } else {
                    logger.info(s"${instance.id} call back online api success")
                    i = i + 100
                  }
                  logger.info(s"${instance.id} end to post result to online")
                }
            }
          //任务抢占失败 放弃任务
          case false =>
        }
      } else {
        logger.info("no need run ")
      }


      sender ! instance.id

    case _ =>
      logger.info("can`t match any receive")
      logger.info("sqlWorker end")

  }

  def clearPath(path: String): Unit = {
    val fs = FileSystem.get(SparkInstance.getSparkSession.sparkContext.hadoopConfiguration)
    val exists = fs.exists(new Path(path))
    if (exists == true) {
      fs.delete(new Path(path), true)
    }
  }

  /**
    * 调度SQL计算工作
    *
    * @param tableName
    * @param instance
    * @param q
    * @param modeType
    * @param csvPath
    * @param parquetPath
    */
  def doSQLJob(tableName: String,
               instance: SQLInstance,
               q: String,
               modeType: org.apache.spark.sql.SaveMode,
               csvPath: String,
               parquetPath: String): Unit = {
    val spark = SparkInstance.getSparkSession
    val sqlStart = (new Date()).getTime / 1000
    var result = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(List(StructField("dmp_id", StringType), StructField("cookie", StringType))))
    if (q.contains("app_table") || q.contains("it_search")) {
      var jobName = ""
      if (instance.business_name == "cct-query") {
        jobName = s"[CCT] [${tableName}] id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
      } else if (instance.business_name == "dmee-baidu") {
        jobName = s"[DMEE-BAIDU] [${tableName}] id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
      } else if (instance.business_name == "dmee-customer") {
        jobName = s"[DMEE-CUSTOMER] [${tableName}] id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
      }
      SparkInstance.getSparkSession.sparkContext.setJobDescription(jobName)


      result = SparkInstance.getSQLContext.sql(q).limit(20000000).toDF("dmp_id", "cookie")


    } else if (q.toUpperCase().contains("INTERSECT") || q.toUpperCase().contains("EXCEPT")) {


      //      val repeatedInstance = instance
      //
      //      val currentBusinessId = repeatedInstance.business_id + "_i_"
      //
      //      if (!q.contains("EXCEPT")){
      //        val list = q.split("INTERSECT")
      //        result = (for (i <- 0 until list.length) yield {
      //          repeatedInstance.business_id = currentBusinessId + i
      //          processSQL(repeatedInstance,tableName,list(i).trim)
      //        }) reduce (_ intersect _)
      //      } else  if (!q.contains("INTERSECT")){
      //        val list = q.split("EXCEPT")
      //        result = (for (i <- 0 until list.length) yield {
      //          repeatedInstance.business_id = currentBusinessId + i
      //          processSQL(repeatedInstance,tableName,list(i).trim)
      //        }) reduce (_ intersect _)
      //      } else {
      //
      //        val sqlList = new ArrayBuffer[String]()
      //        val sqlNums = new ArrayBuffer[Int]()
      //        sqlNums.append(0)
      //        val exceptLis = q.split("EXCEPT")
      //        for (elem <- exceptLis) {
      //          val interList = elem.split("INTERSECT")
      //          sqlList.appendAll(interList)
      //          sqlNums.append(interList.length + sqlNums.last)
      //        }
      //        sqlNums.remove(0)
      //        var exceptIndex: Int = 0
      //        repeatedInstance.business_id = currentBusinessId + 0
      //        var result: DataFrame = processSQL(repeatedInstance, tableName, sqlList(0).trim)
      //
      //        for (i <- 1 until sqlList.length) {
      //          if (i != sqlNums(exceptIndex)) {
      //            repeatedInstance.business_id = currentBusinessId + i
      //            result = result.intersect(processSQL(repeatedInstance, tableName, sqlList(i).trim))
      //          } else {
      //            result = result.except(processSQL(repeatedInstance, tableName, sqlList(i).trim))
      //            exceptIndex = exceptIndex + 1
      //          }
      //        }
      //      }

      val originalBusnessid = instance.business_id

      val repeatedInstance = instance

      var currentBusinessId = repeatedInstance.business_id + "_i_"
      var index: Int = 0
      if (q.contains("EXCEPT")) {

        logger.info(s"run except ${instance.business_id}")
        val exceptList = q.split("EXCEPT")
        result = (for (elem <- exceptList) yield {

          val list = elem.split("INTERSECT")

          (for (elem <- list) yield {

            repeatedInstance.business_id = currentBusinessId + index
            logger.info(s"current businessid ${repeatedInstance.business_id}")
            val tempResult = processSQL(repeatedInstance, tableName, elem.trim)
            index = index + 1
            tempResult
          }) reduce (_ intersect _)

        }) reduce (_ except _)
      } else {

        logger.info(s"run intersect ${instance.business_id}")
        val list = q.split("INTERSECT")
        result = (for (elem <- list) yield {

          repeatedInstance.business_id = currentBusinessId + index
          logger.info(s"current businessid ${repeatedInstance.business_id}")
          val tempResult = processSQL(repeatedInstance, tableName, elem.trim)
          index = index + 1
          tempResult
        }) reduce (_ intersect _)
      }

      instance.business_id = originalBusnessid

    } else {
      //  logger.info(s"dmpid: ${instance.business_id} sql id: ${instance.sql.substring(7,26)}")
      result = processSQL(instance, tableName, q)
    }
    var count: Long = 0

    if (instance.business_name == "dmp-query") {
      result.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER_2)
      val sqlEnd = (new Date()).getTime / 1000
      val sqlUsed = sqlEnd - sqlStart
      result.coalesce(10).write
        .mode(modeType)
        .format("csv")
        .option("delimiter", "\t")
        .save(csvPath)
      //.save(instance.path)
      val hdfsEnd = (new Date()).getTime / 1000
      val hdfsUsed = hdfsEnd - sqlEnd
      //write to bigpipe
      val timestamp = (new Date()).getTime / 1000
      try {
        logger.info(s"[BIGPIPE START][instanceid ${instance.id}]")
        result.foreach { item =>
          BigpipeWriter.write(s"${timestamp}\t%s\t%s".format(item.getAs[String]("dmp_id"),
            item.getAs[String]("cookie")))
        }
      } catch {
        case e: Exception =>
          logger.error(s"[BIGPIPE ERR] [instanceid ${instance.id}] ${e}")
      }

      if (instance.sql.indexOf("dmp_url_table") > 0) {
        count = result.count() * 10
      } else {
        count = result.count()
      }
      logger.info(s"${instance.business_id} ${instance.yymmdd} count $count")

      result.unpersist
      val bigpipeend = (new Date()).getTime / 1000
      val bigpipeused = bigpipeend - timestamp
      logger.info(s"[TIME] [BIGPIPE] [instanceid ${instance.id}] [usetime $bigpipeused]")

      HttpClient.setSQLInstanceStatus(instance.id, None, Some("copied"), Some(count), Some(""))
    } else if (instance.business_name == "cct-query") {
      val cross_result = result
      //写出文本文件
      result.select(regexp_replace(col("cookie"), "\\s+$", "")).write
        .mode(modeType)
        .format("csv")
        .option("delimiter", "\t")
        .save(csvPath)

      //写出partquet文件，提供cross_table使用
      result.select(regexp_replace(col("cookie"), "\\s+$", "").alias("cookie")).write
        .mode(modeType)
        .format("parquet")
        .save(s"/app/ecom/cm/ods/sql-service/cross_table/yymmdd=${instance.yymmdd}/dmp_id=${instance.business_id}")

      if (instance.sql.indexOf("dmp_url_table") > 0) {
        count = result.count() * 10
      } else {
        count = result.count()
      }
      logger.info(s"${instance.business_id} ${instance.yymmdd} count $count")
      HttpClient.setSQLInstanceStatus(instance.id, None, Some("copied"), Some(count), Some(""))


      var i = 0
      while (i < 3) {
        logger.info(s"${instance.id} start to post result to online")
        val flag = HttpClient.postSQLResultToWeb(instance.business_id.toLong, instance.yymmdd, "0", count)
        if (flag == false) {
          logger.warn(s"${instance.id} call back online api failed")
          i = i + 1
          Thread.sleep(3)
        } else {
          logger.info(s"${instance.id} call back online api success")
          i = i + 100
        }
        logger.info(s"${instance.id} end to post result to online")
      }
    } else if (instance.business_name == "dmee-baidu" || instance.business_name == "dmee-customer") {
      //写出文本文件
      result.select(regexp_replace(col("cookie"), "\\s+$", "")).write
        .mode(modeType)
        .format("csv")
        .option("delimiter", "\t")
        .save(csvPath)

      if (instance.sql.indexOf("dmp_url_table") > 0) {
        count = result.count() * 10
      } else {
        count = result.count()
      }
      logger.info(s"${instance.business_id} ${instance.yymmdd} count $count")
      HttpClient.setSQLInstanceStatus(instance.id, None, Some("copied"), Some(count), Some(""))
      var i = 0
      while (i < 3) {
        logger.info(s"${instance.id} start to post result to online")
        val flag = HttpClient.postSQLResultToDMEE(instance.business_id.toLong,
          instance.business_name,
          instance.yymmdd,
          "0",
          count)
        if (flag == false) {
          logger.warn(s"${instance.id} call back online api failed")
          i = i + 1
          Thread.sleep(10)
        } else {
          logger.info(s"${instance.id} call back online api success")
          i = i + 100
        }
        logger.info(s"${instance.id} end to post result to online")
      }
    }

  }

  /**
    * 解析sql，并开始计算
    *
    * @param instance
    * @param tableName
    * @param q
    * @return
    */
  def processSQL(instance: SQLInstance, tableName: String, q: String): DataFrame = {
    // 1.sql 拆分,
    val re =
      """event_day (.*?) days_ago_by_instance\((\d+),.*?\)""".r

    val it = re findAllIn q toList
    var event_day_str = ""
    if (!it.isEmpty) {
      if (it.length == 1) {
        event_day_str = it.mkString("")
      } else if (it.length == 2) {
        event_day_str = it.mkString(" AND ")
      }
    }
    var start_delt = 0
    var end_delt = 0
    val gt = """.*>=.*\((\d+),.*""".r
    val lt = """.*<=.*\((\d+),.*""".r
    it.foreach {
      case gt(delt) => start_delt = delt.toInt
      case lt(delt) => end_delt = delt.toInt
    }
    val sql_daily = q.replace(event_day_str, "event_day = ${daily}")
    val start_yymmdd = getDaysAgoByTable(tableName, start_delt)
    val end_yymmdd = getDaysAgoByTable(tableName, end_delt)

    logger.info(s"${instance.business_id}: start_time: ${start_yymmdd}, end_time: ${end_yymmdd}")
    var changed = false
    // 2. check daily 表
    // 1) 检查单日结果是否可用 2） 缺少sql是否变化的签名。
    var daysNotDone: String = null
    val signature = md5Hash(sql_daily)
    var needToSQL: String = start_yymmdd
    val calendar = Calendar.getInstance()
    //getTableDailyNotDone这个接口返回今天之前的，需要加一天
    calendar.add(Calendar.DAY_OF_YEAR, 1)
    val endTime = format.format(calendar.getTime)


    if (instance.sql.indexOf("weibo_dav_fans") > 0) {
      HttpClient.getTableDailyNotDone(instance.business_id, "20171201", "20171202") match {

        case Some(day) => {
          logger.info("sql weibo")
          handleBack(day, tableName, sql_daily, instance, signature)
        }
        case None =>
      }
      handleMerge(instance, start_yymmdd, "20171202")

    } else {
      //从今天到过去一个月的时间
      HttpClient.getTableDailyNotDone(instance.business_id, start_yymmdd, endTime) match {
        case Some(dailyNotDone) =>
          daysNotDone = dailyNotDone
          if (changed) {
            //sql变化了，所有的单日计算结果需要重新计算
            needToSQL = everyDay(start_yymmdd, end_yymmdd)
          } else {
            needToSQL = daysNotDone
          }
          handleBack(dailyNotDone, tableName, sql_daily, instance, signature)

        case None =>
          logger.info("getTableDailyNotDone returns None")
      }
      handleMerge(instance, start_yymmdd, endTime)

    }

  }

  /**
    * 将已经计算的单日结果merge
    *
    * @param instance
    * @param start_yymmdd
    * @param end_yymmdd
    * @return
    */
  def handleMerge(instance: SQLInstance, start_yymmdd: String, end_yymmdd: String): DataFrame = {
    val dmp_id = instance.business_id
    logger.info(s"${dmp_id} merge start")
    //注册单日结果表
    val tmp_table = s"${dmp_id}_daily_result"
    val path = s"${dailyPath}/dmpid=${dmp_id}"
    val spark = SparkInstance.getSparkSession()
    val alldays = everyDay(start_yymmdd, end_yymmdd).split(",")
    var days = alldays
    HttpClient.getTableDailyNotDone(dmp_id, start_yymmdd, end_yymmdd) match {
      case Some(notday) => {
        logger.info("unfinish daily day " + notday)
        days = alldays.diff(notday.split(","))
      }
      case _ =>
    }
    if (days != null && days.length > 0) {
      ((for (day <- days) yield {
        var jobName = ""
        if (instance.business_name == "cct-query") {
          jobName = s"[CCT] [Merge Read Daily] y=$day id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
        } else if (instance.business_name == "dmee-baidu") {
          jobName = s"[DMEE-BAIDU] [Merge Read Daily]  y=$day  id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
        } else if (instance.business_name == "dmee-customer") {
          jobName = s"[DMEE-CUSTOMER] [Merge Read Daily ] y=$day  id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
        }
        SparkInstance.getSparkSession.sparkContext.setJobDescription(jobName)
        try {
          spark.read.parquet(path + "/event_day=" + day)
        } catch {
          case e: Exception =>
            logger.info(e.toString)
            null
        }
      }) reduce (_ union _)).toDF("dmp_id", "cookie").createOrReplaceTempView(tmp_table)


      val sql = s"select distinct dmp_id, cookie from ${tmp_table} "

      var jobName = ""
      if (instance.business_name == "cct-query") {
        jobName = s"[CCT] [Merge] y=$start_yymmdd-$end_yymmdd id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
      } else if (instance.business_name == "dmee-baidu") {
        jobName = s"[DMEE-BAIDU] [Merge]  y=$start_yymmdd-$end_yymmdd  id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
      } else if (instance.business_name == "dmee-customer") {
        jobName = s"[DMEE-CUSTOMER] [Merge] y=$start_yymmdd-$end_yymmdd  id=${instance.id} bus_n=${instance.business_name} bus_id=${instance.business_id}"
      }
      SparkInstance.getSparkSession.sparkContext.setJobDescription(jobName)

      val result = SparkInstance.getSparkSession().sql(sql).limit(20000000).toDF("dmp_id", "cookie");
      logger.info(s"${dmp_id} merge end")
      result
    } else {
      logger.info(s"${dmp_id} no day to merge")
      null
    }
  }

  /**
    * 回溯没有计算的单日结果
    *
    * @param dailyNotDone
    * @param tableName
    * @param sqlDaily
    * @param instance
    * @param signature
    */
  def handleBack(dailyNotDone: String, tableName: String, sqlDaily: String, instance: SQLInstance, signature: String): Unit = {
    val dmp_id = instance.business_id
    HttpClient.getTableSrcDone(tableName, dailyNotDone) match {
      case Some(days) =>
        val daysList = days.split(",").reverse
        logger.info(s"daily not done ${days}")
        if (daysList.length >= 15) {
          logger.info(s"${instance.business_id} many days $dailyNotDone")
        }
        daysList.foreach(day => {
          var jobName = ""
          if (instance.business_name == "cct-query") {
            jobName = s"[CCT] [Daily] y=${day} id=[${instance.id}] [${tableName}] bus_n=${instance.business_name} bus_id=${instance.business_id}"
          } else if (instance.business_name == "dmee-baidu") {
            jobName = s"[DMEE-BAIDU] [Daily] y=${day} id=[${instance.id}] [${tableName}] bus_n=${instance.business_name} bus_id=${instance.business_id}"
          } else if (instance.business_name == "dmee-customer") {
            jobName = s"[DMEE-CUSTOMER] [Daily] y=${day} id=[${instance.id}] [${tableName}] bus_n=${instance.business_name} bus_id=${instance.business_id}"
          }
          SparkInstance.getSparkSession.sparkContext.setJobDescription(jobName)
          handleDaily(day, sqlDaily, instance.business_id, signature)
        })
      case None =>
        logger.info("getTableSrcDone returns None")
    }
  }

  /**
    * 计算单日结果
    *
    * @param day
    * @param sqlDaily
    * @param dmp_id
    * @param signature
    */
  def handleDaily(day: String, sqlDaily: String, dmp_id: String, signature: String): Unit = {
    if (day == null || day.equals("")) {
      logger.info(s"$dmp_id lost day")
      return
    }
    logger.info(s"$dmp_id daily start ${day}")
    val path = s"$dailyPath/dmpid=$dmp_id/event_day=$day"
    val sql = sqlDaily.replace("${daily}", day)

    try {
      val rst = SparkInstance.getSparkSession().sql(sql).limit(20000000).toDF("dmp_id", "cookie")
      //保存 daily 结果 path
      rst.coalesce(10).write
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .format("parquet")
        .save(path)
      HttpClient.setTableDailyStatus(dmp_id, day, "success")
      logger.info(s"$dmp_id daily end $day")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.info(s"daily sql failure $dmp_id $day ")
    }
  }

  def getDaysAgoByTable(table: String, delt: Int) = {
    var date: String = ""
    val format = new SimpleDateFormat("yyyyMMdd")
    Utils.getTableVersion(table) match {
      case Some(version) =>
        // logger.info(s"table $table, version $version")
        date = version
      case None =>
        val cal: Calendar = Calendar.getInstance()
        date = format.format(cal.getTime())
    }


    val current = format.parse(date)
    val calendar = Calendar.getInstance()
    calendar.setTime(current)
    for (x <- 0 until delt) {
      calendar.add(Calendar.DAY_OF_YEAR, -1)
    }
    format.format(calendar.getTime())
  }

  override def preStart(): Unit = {
    logger.info("sqlWorker start")
  }

  override def postStop(): Unit = {
    logger.info("sqlWorker stop")
  }

  /**
    * MD5 签名生成函数
    *
    * @param text
    * @return
    */
  def md5Hash(text: String): String =
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }

  /**
    * 给定日期经过几天后是那一天
    *
    * @param date      给定日期
    * @param increment 经过多少天
    * @return
    */
  def dateTransform(date: String, increment: Int): String = {
    try {
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val cd = Calendar.getInstance
      cd.setTime(sdf.parse(date))
      cd.add(Calendar.DATE, increment)
      sdf.format(cd.getTime)
    } catch {
      case e: Exception => null
    }
  }

  /**
    * 两个日期隔了多少天
    *
    * @param start
    * @param end
    * @return
    */
  def dateInterval(start: String, end: String): Int = {
    if (start == null || end == null || start.equals("") || end.equals(""))
      0
    val sdf = new SimpleDateFormat("yyyyMMdd")
    try {
      val endInt = sdf.parse(end).getTime
      val startInt = sdf.parse(start).getTime
      if (endInt <= startInt) {
        0
      } else {
        ((sdf.parse(end).getTime - sdf.parse(start).getTime) / (1000 * 3600 * 24)).asInstanceOf[Int]
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        0
    }
  }

  /**
    * 开始到结束的每一天，逗号隔开
    *
    * @param start
    * @param end
    * @return
    */
  def everyDay(start: String, end: String): String = {
    if (start == null || end == null) {
      return null
    }
    var needToSQL = start
    val interval = dateInterval(start, end)
    for (i <- 1 until interval) {
      needToSQL = needToSQL + "," + dateTransform(start, i)
    }
    needToSQL
  }
}