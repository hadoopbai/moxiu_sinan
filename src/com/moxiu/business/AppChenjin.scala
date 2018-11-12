package com.moxiu.business

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * 实现业务逻辑：
 * 1、找出自2015～至2017-06-24所有的桌面用户中，并且最近8天（2017-07-17〜2017-07-24）有数据交互的用户，取别名A。
 * 2、找出所有按装爱奇艺app用户，与上一步取交集，取别名B。（意为A中装爱奇艺app的用户）
 * 3、找出2017-06-24至2017-07-24一个月内打开爱奇艺app的用户，取别名C。
 * 4、沉寂用户 D = B-C
 */
object AppChenjin {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName("AppChenjin")
    val separator = "\001"

    //留存表 
    val rdd_xrr = sc.textFile("/user/jp-spark/liucun_process/launcher/data/liucun/2017/07/24/*").map(line => {
      val array = line.split(separator)
      // userId，startDate,lastDate
      (array(0), array(1), array(2))
    }).filter(f => f._2 < "2017-06-24" && f._3 > "2017-07-16").map(_._1)
    rdd_xrr.persist
    //--------按装爱奇艺用户-------
    val all_appPath = "/cleandata/apps/549c6462ba4d9b4d098b4567/biz/allapps/2017/*/*/online/*"
    // userId
    val rdd_allApp = sc.textFile(all_appPath).map(line => line.split(separator)).filter(f => (f.size > 30 && f(30).trim == "com.qiyi.video")).map(_(4))
    rdd_allApp.persist
    val rdd_moxiu_aiqiyi = rdd_xrr.intersection(rdd_allApp)
    rdd_xrr.unpersist()
    rdd_allApp.unpersist()
    //--------1月内打开爱奇艺app用户-------
    val open_appPath = "/cleandata/apps/549c6462ba4d9b4d098b4567/biz/openapp/2017/0[6-7]/*/online/*"
    val rdd_openApp = sc.textFile(all_appPath).map(_.split(separator)).filter(f => (f.size >= 23 && f(1) >= "2017-06-24" && f(23).trim == "com.qiyi.video")).map(f => (f(0), 1)).reduceByKey(_ + _).map(_._1)

    //--------去除未失活用户并且按装爱奇艺app用户-------
    rdd_moxiu_aiqiyi.subtract(rdd_openApp).map(f => (f.split("\\|")(1), 1)).filter(f => f._1.trim != "null").reduceByKey(_ + _).map(_._1).repartition(1).saveAsTextFile("/user/jp-spark/liucun_process/launcher/data/aiqiyi")
  }
}