package app.ecom.dmp.search

import akka.actor.Actor
import akka.actor.Props
import com.typesafe.scalalogging.LazyLogging

//建立临时表 刷新临时表
class RefreshTableTimeActor extends Actor with LazyLogging {

  val spark = SparkInstance.getSparkSession
  val tables = collection.mutable.Map[String, Boolean]()

  //初始化
  override def preStart() {
    logger.info("RefreshTableTimeActor preStart.")
  }

  def receive = {
    case "updateTableTime" =>
      logger.info("updateTableTime")
      "app_table,holmes_unknown_view,dmp_url_table,iqiyi_search,ei_query,ei_page,fc_click,baiyi_search,it_search,weibo_dav_fans".split(",").foreach(table => {
        //logger.info(s"table: ${table}")
        //        if (table(0).asInstanceOf[Boolean] == true) {
        HttpClient.getTableVersion(table) match {
          case Some(version) =>
            Utils.updateTableVersion(table, version)
           // logger.info(s"tables: ${table} version: ${version}")
          case None =>
        }
        //        }
      })

    case _ =>
  }


}
