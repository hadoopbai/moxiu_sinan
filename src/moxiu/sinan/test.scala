package moxiu.sinan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField


object test {
  def main(args: Array[String]) {
//    val spark = SparkSession.builder.appName("ErrorTrend").getOrCreate()
//     // read json data from hdfs
//    val appMark = spark.read.json("/sources/web/mongodb/app_util/2017/08/29")
//    appMark.createOrReplaceTempView("appMark")
//    appMark.printSchema()
//    
//     val df_errorTable = spark.sql("select appname,ctime,isSystem,package,"+
//         "packagepvuc.pv_count,packagepvuc.update_time, packagepvuc.uv_count,userCount,"+
//         "markets"+
////         "markets.360market.apkurl,markets.360market.app_downcount,markets.360market.appname,markets.360market.categoryid,markets.360market.categoryname,markets.360market.marketname,markets.360market.publish_time,markets.360market.size,"+
////     " markets.360market.tags.element,markets.360market.update_time,markets.360market.vcode,markets.360market.vname,"+
////              "markets.baidumarket.apkurl,markets.baidumarket.app_downcount,markets.baidumarket.appname,markets.baidumarket.categoryid,markets.baidumarket.categoryname,markets.baidumarket.marketname,markets.baidumarket.publish_time,markets.baidumarket.size,"+
////     " markets.baidumarket.tags.element,markets.baidumarket.update_time,markets.baidumarket.vcode,markets.baidumarket.vname,"+
////              "markets.tencentmarket.apkurl,markets.tencentmarket.app_downcount,markets.tencentmarket.appname,markets.tencentmarket.categoryid,markets.tencentmarket.categoryname,markets.tencentmarket.marketname,markets.tencentmarket.publish_time,markets.tencentmarket.size,"+
////     " markets.tencentmarket.tags.element,markets.tencentmarket.update_time,markets.tencentmarket.vcode,markets.tencentmarket.vname"+
//     " FROM appMark")
//     df_errorTable.rdd.saveAsTextFile("/user/jp-spark/mongodb")
     
//     
//         // 定义schema
//    val schemaString = "id appname ctime isSystem package pv_count update_time uv_count userCount 360apkurl 360app_downcount 360appname 360categoryid 360categoryname 360name 360publish_time 360size "+
//    "baidumarketapkurl baidumarketapp_downcount baidumarketappname baidumarketcategoryid baidumarketcategoryname baidumarketmarketname baidumarketpublish_time baidumarketsize baidumarkettagselement baidumarketupdate_time baidumarketvcode baidumarketvname "+
//    "tencentmarketapkurl tencentmarketapp_downcount tencentmarketappname tencentmarketcategoryid tencentmarketcategoryname tencentmarketmarketname tencentmarketpublish_time tencentmarketsize tencentmarkettagselement tencentmarketupdate_time tencentmarketvcode tencentmarketvname"
//    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(fields)
     
//    df_errorTable.persist
//    df_errorTable.createOrReplaceTempView("channel_ver")
     
     
//    root
// |-- _id: struct (nullable = true)
// |    |-- $oid: string (nullable = true)
// |-- appname: string (nullable = true)
// |-- ctime: string (nullable = true)
// |-- isSystem: boolean (nullable = true)
// |-- markets: struct (nullable = true)
// |    |-- 360market: struct (nullable = true)
// |    |    |-- apkurl: string (nullable = true)
// |    |    |-- app_downcount: string (nullable = true)
// |    |    |-- appname: string (nullable = true)
// |    |    |-- categoryid: string (nullable = true)
// |    |    |-- categoryname: string (nullable = true)
// |    |    |-- description: string (nullable = true)
// |    |    |-- marketname: string (nullable = true)
// |    |    |-- publish_time: string (nullable = true)
// |    |    |-- size: string (nullable = true)
// |    |    |-- tags: array (nullable = true)
// |    |    |    |-- element: string (containsNull = true)
// |    |    |-- update_time: string (nullable = true)
// |    |    |-- vcode: string (nullable = true)
// |    |    |-- vname: string (nullable = true)
// |    |-- baidumarket: struct (nullable = true)
// |    |    |-- apkurl: string (nullable = true)
// |    |    |-- app_downcount: string (nullable = true)
// |    |    |-- appname: string (nullable = true)
// |    |    |-- categoryid: string (nullable = true)
// |    |    |-- categoryname: string (nullable = true)
// |    |    |-- description: string (nullable = true)
// |    |    |-- marketname: string (nullable = true)
// |    |    |-- publish_time: string (nullable = true)
// |    |    |-- size: string (nullable = true)
// |    |    |-- tags: array (nullable = true)
// |    |    |    |-- element: string (containsNull = true)
// |    |    |-- update_time: string (nullable = true)
// |    |    |-- vcode: string (nullable = true)
// |    |    |-- vname: string (nullable = true)
// |    |-- tencentmarket: struct (nullable = true)
// |    |    |-- apkurl: string (nullable = true)
// |    |    |-- app_downcount: string (nullable = true)
// |    |    |-- appname: string (nullable = true)
// |    |    |-- categoryid: string (nullable = true)
// |    |    |-- categoryname: string (nullable = true)
// |    |    |-- description: string (nullable = true)
// |    |    |-- marketname: string (nullable = true)
// |    |    |-- publish_time: string (nullable = true)
// |    |    |-- size: string (nullable = true)
// |    |    |-- tags: string (nullable = true)
// |    |    |-- update_time: string (nullable = true)
// |    |    |-- vcode: string (nullable = true)
// |    |    |-- vname: string (nullable = true)
// |-- package: string (nullable = true)
// |-- packagepvuc: struct (nullable = true)
// |    |-- pv_count: string (nullable = true)
// |    |-- pv_uv_count: string (nullable = true)
// |    |-- update_time: string (nullable = true)
// |    |-- uv_count: string (nullable = true)
// |-- userCount: long (nullable = true)
     
//       val r = "^[0-9|a-z|A-Z]".r
//     
//     val ss="sfsfdsfd"
//    Console println  ss.matches("^[0-9|a-z|A-Z].*")
     
    val ssrt  = 1 to 5 map(f=>"?")
      Console println  ssrt.mkString("(",",",")")
      
  }
}