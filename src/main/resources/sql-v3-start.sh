#!/bin/sh

#p_sql=`ps -ef|grep thisdirisnoreplicatedformaxu03othersnouse|grep -v grep |awk '{print $2}'`
p_sql=`ps -ef | grep sql-service | grep -v grep | awk '{print $2}'`
for i in $p_sql
do
	kill -9 $i
    echo $i
done
cur_day=`date +%Y%m%d`
hour=`date +"%H"`
#if [ $hour -lt 18 ]; then
#get error sql to back
#`mysql -hyq01-dmp-spark-master.yq01 -ularavel -plaravel laravel -N -e "update sql_instances set status='new',error_message='' where yymmdd=$cur_day and status='error'"`
#fi
#update searching sql to back

`mysql -hyq01-dmp-spark-master.yq01 -ularavel -plaravel laravel -N -e "update sql_instances set status='new',error_message='' where status='searching'"`

sleep 3
`spark-submit --master yarn --class app.ecom.dmp.search.Main --driver-memory=40g --num-executors=800  --executor-memory=15g --executor-cores=3 --conf spark.eventLog.enabled=false --conf spark.local.dir=/home/disk1/spark_local/   --conf spark.driver.maxResultSize=10g    --conf spark.ui.retainedJobs=20 --conf spark.ui.retainedStages=40 --conf spark.sql.ui.retainedExecutions=10 --conf spark.yarn.executor.memoryOverhead=2048  --conf spark.executor.heartbeatinterval=60s --conf spark.network.timeout=800 --conf spark.rpc.askTimeout=1000 --conf spark.sql.broadcasttimeout=10000 --conf spark.dynamicallocation.enabled=false --conf spark.shuffle.io.maxRetries=8 --conf spark.broadcast.blockSize=32m --conf spark.sql.shuffle.partitions=4000 --conf spark.default.parallelism=4000 --conf spark.memory.storageFraction=0.4 --jars /home/work/wangchao/folder/akka-actor_2.11-2.3.9.jar,/home/work/wangchao/folder/akka-slf4j_2.11-2.3.9.jar,/home/work/wangchao/folder/config-1.2.1.jar,/home/work/wangchao/folder/scalaj-http_2.11-2.3.0.jar,/home/work/wangchao/folder/scala-logging_2.11-3.5.0.jar,/home/work/wangchao/folder/bigpipe4j-1.3.5.3.jar,/home/work/wangchao/folder/json4s-native_2.11-3.4.0.jar,/home/work/wangchao/folder/logback-classic-1.1.3.jar,/home/work/wangchao/folder/json4s-ast_2.11-3.4.0.jar  /home/work/wangchao/alltable/sql-service-delta_2.11-0.1.jar`


#`SPARK_MAJOR_VERSION=2 spark-submit --master yarn --conf "spark.executor.extraJavaOptions= -XX:+UseG1GC -verbose:gc -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=20 -Dfile.encoding=UTF-8" --conf "spark.driver.extraJavaOptions= -XX:+UseG1GC  -verbose:gc  -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=20 -Dfile.encoding=UTF-8 " --queue yq01-online --conf "spark.shuffle.service.enabled=false" --conf "spark.dynamicAllocation.enabled=false" --conf "spark.driver.memory=64g" --conf "spark.driver.maxResultSize=10G" --conf "spark.executor.instances=310" --conf "spark.yarn.executor.memoryOverhead=4096" --conf "spark.speculation=true" --conf "spark.hadoop.fs.hdfs.impl.disable.cache=true" thisdirisnoreplicatedformaxu03othersnouse/sql-service-assembly-1.0.jar`
