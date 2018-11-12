package com.moxiu.calculate.sinan.launcher

import moxiu.sinan.DimensionCore
import com.moxiu.shucang.sinan.MoxiuUtil
import org.apache.spark.sql.types.{ StructField, StructType, StringType }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
 * 司南-错误趋势分析
 * @author 宿荣全
 * @date 2017-08-23
 * 按版本渠道计算错误日志的PV，UV，以及UV／DAU
 */
object ErrorTrend {
  def main(args: Array[String]): Unit = {
    // 日维度表路径（除时间部分，无最后的“／”）
    val dimensionRootPath = args(0).trim
    // 日维度表路径（除时间部分，无最后的“／”）
    val errorPathRootPath = args(1).trim
    // 当前天数据日期（当前天-1）
    val dataDate = args(2).trim
    // 产品名称:launcher|vlock
    val productName = args(3).trim
    // 是否打印表数据到文件
    val print_flg = args(4).trim.toBoolean

    val separator = "\001"
    // 日纬度表： 0:userId,1:ctime,2:版本,3:渠道，4:品牌，5:机型，6:网络，7:分辨率，8:安卓版本，9:省，10:市，11:运营商
    val dimensionPath = dimensionRootPath + "/" + MoxiuUtil.getDate(dataDate, 0, "yyyy/MM/dd")
    val errorPathPath = errorPathRootPath + "/" + MoxiuUtil.getDate(dataDate, 0, "yy/MM/dd")

    // -----------------数据表预删除处理------------------------------------
    val vlock_tableList = Array("_daily_trend", "_channel_ver_trend", "_channel_trend", "_ver_trend")
    MoxiuUtil.deleTable(productName, dataDate, vlock_tableList, 1)

    val spark = SparkSession.builder.appName("ErrorTrend").getOrCreate()

    // read json data from hdfs
    val errMsgJson = spark.read.json(errorPathPath)
    errMsgJson.createOrReplaceTempView("errorTable")
    // muuid,child,ver
    val df_errorTable = spark.sql("select  concat_ws('|',case trim(base.androidid) when '' then 'null' else trim(base.androidid) end,case trim(base.imei) when '' then 'null' else trim(base.imei) end) as muuid ,base.child,content.ver FROM errorTable")
    df_errorTable.persist
    df_errorTable.createOrReplaceTempView("channel_ver")
    // 错误日志的pv,uv
    val pv_uv = spark.sql("select count(muuid),count(distinct muuid) FROM channel_ver").rdd.map(f => (f.getLong(0), f.getLong(1))).collect()
    // 按版本渠道统计pv,uv
    val ch_ver_puv = spark.sql("select count(muuid),count(distinct muuid),ver,child FROM channel_ver where ver is not null and child is not null  group by ver,child").rdd.map(f =>
      (f.getAs[String]("ver") + separator + f.getAs[String]("child"), Array(f.getLong(0), f.getLong(1))))
    // 按渠道统计pv,uv
    val channel_puv = spark.sql("select count(muuid),count(distinct muuid),child  FROM channel_ver where child is not null group by child").rdd.map(f =>
      (f.getAs[String]("child"), Array(f.getLong(0), f.getLong(1))))
    // 按版本统计pv,uv
    val ver_puv = spark.sql("select count(muuid),count(distinct muuid),ver  FROM channel_ver where ver is not null group by ver").rdd.map(f => (f.getAs[String]("ver"), Array(f.getLong(0), f.getLong(1))))
    df_errorTable.unpersist()

    // ----------------维度表计算------------------------------
    // 定义schema
    val schemaString = "muuid ver child"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // 日纬度表： 0:userId,1:ctime,2:版本,3:渠道，4:品牌，5:机型，6:网络，7:分辨率，8:安卓版本，9:省，10:市，11:运营商
    val weidu_rdd = DimensionCore.getRDD(spark.sparkContext, dimensionPath).map(array => (array(0), array(2), array(3)))
    val rowRDD = weidu_rdd.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    rowRDD.persist
    spark.createDataFrame(rowRDD, schema).createOrReplaceTempView("dimension_table")
    // 日活
    val dau = spark.sql("select count(distinct muuid) FROM dimension_table").rdd.map(_.getLong(0)).collect()
    // 按版本渠道统计日活
    val ch_ver_dau = spark.sql("select count(distinct muuid),ver,child FROM dimension_table group by ver,child").rdd.map(f =>
      (f.getAs[String]("ver") + separator + f.getAs[String]("child"), f.getLong(0)))
    // 按渠道统计pv,uv
    val channel_dau = spark.sql("select count(distinct muuid),child  FROM dimension_table group by child").rdd.map(f =>
      (f.getAs[String]("child"), f.getLong(0)))
    // 按版本统计pv,uv
    val ver_dau = spark.sql("select count(distinct muuid),ver  FROM dimension_table group by ver").rdd.map(f =>
      (f.getAs[String]("ver"), f.getLong(0)))

    // ----------------结果输出------------------------------
    val pv = pv_uv(0)._1
    val uv = pv_uv(0)._2
    val rate = uv / dau(0).toDouble
    val totail_puv = Array(Array(dataDate, pv.toString, uv.toString, rate.toString))
    // UV >1 的有效
    val ch_ver_puv_result = ch_ver_puv.leftOuterJoin(ch_ver_dau).map(line => output_result(dataDate, line, line._1.split(separator)(0), line._1.split(separator,2)(1))).filter(array=>array(4).size >1).collect()
    val channel_dau_result = channel_puv.leftOuterJoin(channel_dau).map(line => output_result(dataDate, line, line._1)).filter(array=>array(3).size >1).collect()
    val ver_dau_result = ver_puv.leftOuterJoin(ver_dau).map(line => output_result(dataDate, line, line._1)).filter(array=>array(3).size >1).collect()
    spark.stop
    val msg = "----- 司南-错误趋势分析 launcher_daily_trend------"
    val items = Array("DATE", "PV", "UV", "RATE")
    MoxiuUtil.updatePrint(print_flg, productName, "_daily_trend", dataDate, totail_puv, items, msg)
    val msg1 = "------------launcher_channel_ver_trend--------------"
    val ch_ver_puv_item = Array("DATE", "VER", "CHANNEL", "PV", "UV", "RATE")
    MoxiuUtil.updatePrint(print_flg, productName, "_channel_ver_trend", dataDate, ch_ver_puv_result, ch_ver_puv_item, msg1)
    val msg2 = "-----------launcher_channel_trend------------------"
    val channel_item = Array("DATE", "CHANNEL", "PV", "UV", "RATE")
    MoxiuUtil.updatePrint(print_flg, productName, "_channel_trend", dataDate, channel_dau_result, channel_item, msg2)
    val msg3 = "------------launcher_ver_trend----------------------"
    val ver_item = Array("DATE", "VER", "PV", "UV", "RATE")
    MoxiuUtil.updatePrint(print_flg, productName, "_ver_trend", dataDate, ver_dau_result, ver_item, msg3)
  }

  /**
   * 结果输出
   */
  def output_result(dataDate: String, line: (String, (Array[Long], Option[Long])), params: String*) = {
    val key = line._1
    val pv = line._2._1(0)
    val uv = line._2._1(1)
    val dau = if (line._2._2 == None) 0 else line._2._2.get
    val array = ArrayBuffer[String]()
    array += dataDate
    for (e <- params) array += e
    array += pv.toString
    array += uv.toString
    array += (if (dau == 0) "0" else (uv / dau.toDouble).toString)
    array.toArray
  }
}