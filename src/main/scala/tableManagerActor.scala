package app.ecom.dmp.search

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging

//建立临时表 刷新临时表
class TableManagerActor extends Actor with LazyLogging {
  //保存表名和路径的映射
  val tables = collection.mutable.Map[String, Traversable[String]]()
  val spark = SparkInstance.getSparkSession

  var i = 0

  //初始化
  override def preStart() {
    logger.info("init all table!!!!!")
    //tables put ("baiyi_search", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=baiyi_search"))
    //tables put ("fc_click", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=fc_click"))
    //tables put("dmp_url_table", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=dmp_url_table"))
    //
    //tables put ("ei_query",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=kaiwu_query_table"))
    //tables put ("it_search", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=it_search"))
    //tables put ("cross_table", Traversable("/user/cm_cosmos/second-result","/user/cm_cosmos/cct-result"))
    //tables put ("iqiyi_search", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=iqiyi_search"))
    //tables put ("ei_query", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=ei_query"))
    //tables put ("ei_page", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=ei_page"))

      tables put ("cross_table",Traversable("/app/ecom/cm/ods/sql-service/cross_table"))
      tables put("dmp_url_table", Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=dmp_url_table"))
      tables put("iqiyi_search", Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=iqiyi_search_table"))
      tables put("app_table", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=app_table"))
      tables put("holmes_unknown_view", Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=holmes_unknown_view_table"))


    //
     tables put ("ei_page",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=kaiwu_page_table"))
     tables put ("ei_query",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=kaiwu_query_table"))
     tables put ("baiyi_search",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=baiyi_search_table"))
    //
     tables put ("fc_click",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=fengchao_click_table"))
     tables put ("it_search",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=it_search_table"))
    tables put ("weibo_dav_fans",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=weibo_table"))


    //tables put("app_table", Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=kaiwu_app_table"))//


    //    tables put ("weibo_dav_fans",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=weibo_table"))
    //
    //    tables put ("ei_page",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=kaiwu_page_table"))
    //
    //    tables put ("ei_query",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=kaiwu_query_table"))
    //
    //    tables put ("baiyi_search",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=baiyi_search_table"))
    //
    //    tables put ("fc_click",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=fengchao_click_table"))
    //    tables put ("it_search",Traversable("/app/ecom/cm/ods/sql-service/source-log/table_name=it_search_table"))


    //tables put ("app_table", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=app_table"))
    //tables put ("weibo_dav_fans", Traversable("/app/ecom/cm/ods/dmp/parquet-data/table_name=weibo_dav_fans"))

    //tables put ("ei_page", Traversable("/etl-data/table_name=page_ei_dmp"))
    //tables put ("holmes_unknown_view", Traversable("/etl-data/table_name=holmes_unknown_view"))
    //tables put ("it_search", Traversable("/etl-data/table_name=it_search_offline"))
    //tables put ("dmp_url_table",Traversable("/etl-data/table_name=dmp_url_table_sample"))
    //tables put ("fc_click", Traversable("/raw-data/table_name=fengchao_pc_web_click", "/raw-data/table_name=fengchao_wap_common_click"))
    //tables put ("qiushi_charge", Traversable("/raw-data/table_name=qiushi_charge"))
    //tables put ("qiushi_display", Traversable("/raw-data/table_name=dayu_qiushi_search_normal_pb"))
    //tables put ("newdsp_click", Traversable("/raw-data/table_name=newdsp_click"))
    //tables put ("newdsp_display", Traversable("/raw-data/table_name=newdsp_display"))
    //tables put ("beidou_click", Traversable("/raw-data/table_name=beidou_click"))
    //tables put ("beidou_display", Traversable("/raw-data/table_name=beidou_display_le","/raw-data/table_name=beidou_display_gt0","/raw-data/table_name=beidou_display_gt1","/raw-data/table_name=beidou_display_non0"))
    //tables put ("holmes_transform_ad_all_key", Traversable("/user/cm_cosmos/dmp-log/table_name=holmes_transform_all_noah"))
    //tables put ("ods_pinzhuan", Traversable("/user/cm_cosmos/udw-log/table_name=pinzhuan_rcv2_adrc_anti_quater_log_hour"))
    //tables put ("ei_page", Traversable("/raw-data/table_name=ei_page"))
    //tables put ("ei_page", Traversable("/etl-data/table_name=ei_page_partition"))
    //tables put ("ei_query", Traversable("/etl-data/table_name=ei_query_partition"))

    //spark.read.orc(path).toDF("event_baiduid","url","url_split","event_day").createOrReplaceTempView(table_name)

    try {

            logger.info("init app_table")
            ((for (path <- tables.get("app_table").get) yield spark.read.parquet(path)) reduce (_ union _)).createOrReplaceTempView("app_table")


      for (path <- tables.get("dmp_url_table").get) {
        logger.info("init dmp_url_table")
        spark.read.orc(path).toDF("event_baiduid", "url", "url_split", "event_day").createOrReplaceTempView("dmp_url_table")
      }
      for (path <- tables.get("iqiyi_search").get) {
        logger.info("init iqiyi_search")
        spark.read.orc(path).toDF("cookie", "iqiyi_channel_name", "iqiyi_types", "iqiyi_album_name", "iqiyi_actor_name", "iqiyi_director_name", "iqiyi_area", "iqiyi_tv_year", "event_day", "iqiyi_from_type").createOrReplaceTempView("iqiyi_search")
      }

      for (path <- tables.get("holmes_unknown_view").get) {
        logger.info("init holmes_unknown_view")
        spark.read.orc(path).toDF("holmes_from_type", "holmes_click_type", "holmes_logtype", "holmes_from_url", "holmes_clickid", "holmes_visit_time", "holmes_pv_view_url", "holmes_site_status", "event_baiduid", "holmes_dmp_pv_life_cycle", "holmes_siteid", "holmes_dmp_visit_bounce", "holmes_is_new_visitor", "holmes_dmp_pv_view_url", "holmes_siteid_partition", "event_day").createOrReplaceTempView("holmes_unknown_view")
      }

            for (path<- tables.get("ei_query").get){
              logger.info("init ei_query")
              spark.read.orc(path).toDF("cookie","freq", "key", "key_partition","event_day").createOrReplaceTempView("ei_query")
            }
            for (path<- tables.get("ei_page").get){
              logger.info("init ei_page")
              spark.read.orc(path).toDF("cookie","freq", "key", "key_partition","event_day").createOrReplaceTempView("ei_page")
            }

            for (path<- tables.get("baiyi_search").get){
              logger.info("init baiyi_search")
              spark.read.orc(path).toDF("cookie","planid","timestamp","unitid","userid","action_type","event_day").createOrReplaceTempView("baiyi_search")
            }

            for (path<- tables.get("it_search").get){
              logger.info("init it_search")

              spark.read.orc(path).toDF("cookie","it","event_day").createOrReplaceTempView("it_search")
            }


            for (path<- tables.get("fc_click").get){
              logger.info("init fc_click")
              spark.read.orc(path).toDF("event_baiduid","fcadclick_ad_user_id","fcadclick_plan_id","fcadclick_unit_id","event_day").createOrReplaceTempView("fc_click")
            }

            for (path <- tables.get("weibo_dav_fans").get) {
              logger.info("init weibo_dav_fans")
              spark.read.orc(path).toDF("uuid", "dav_weibo_userid", "event_day").createOrReplaceTempView("weibo_dav_fans")
            }

            logger.info("init cross_table")
            ((for (path <- tables.get("cross_table").get) yield spark.read.parquet(path)) reduce (_ union _)).toDF("cookie","yyyymmdd","dmp_id").createOrReplaceTempView("cross_table")

//            for (path <- tables.get("cross_table").get){
//
//             spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter","\t").load(path).toDF("cookie","yyyymmdd","dmp_id").createOrReplaceTempView("cross_table")
//            }


//      if (i < 2) {
//        spark.sql("show tables").collect().foreach(each => {
//          logger.info(s"current table ${each}")
//        })
//        i = i + 1
//      }


    } catch {
      case e: Exception =>
        logger.info("create table error")
    }

  }

  def receive = {
    case "refreshTable" =>
      logger.info("refreshTable")
      (tables keys) foreach { table_name =>
        spark.catalog.refreshTable(table_name)
      }
    case _ =>
  }


}
