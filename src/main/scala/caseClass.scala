package app.ecom.dmp.search

import org.apache.spark.sql.{DataFrame, SaveMode}

case class SqlJob(id: Long, sql: String, path: String, yymmdd: String)

case class SqlResult(id: Long, count: Option[Long], exception: Option[Exception])

case class SQLInstance( var id: Long, var sql: String, path: String,  yymmdd: String,
                       business_name: String, var business_id: String)

case class TableVersionRes(message: String, data: String, result: String)

case class MessageBody(status: Int, msg: String, data: String)

case class SQLCondition(instance:SQLInstance,result:DataFrame,modeType:SaveMode,csvPath:String)

case class SQLTimeRange(dailySQL:String,var start:String,end:String)


