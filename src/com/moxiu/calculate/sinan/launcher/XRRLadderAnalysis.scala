package com.moxiu.calculate.sinan.launcher

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import com.moxiu.shucang.sinan.MoxiuUtil
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import moxiu.sinan.DimensionCore
import scala.collection.mutable.Map

/**
 * 梯度留存分析
 */
object XRRLadderAnalysis {

  val separator = "\001"
  def main(args: Array[String]): Unit = {

    // 汇总留存表路径（除时间部分，无最后的“／”）
    val liuCunRootPath = args(0).trim
    // 日维度表路径（除时间部分，无最后的“／”）
    val dimensionRootPath = args(1).trim
    // 当前天数据日期（当前天-1）
    val dataDate = args(2).trim
    // 产品名称:launcher|vlock
    val productName = args(3).trim
    // 是否打印表数据到文件
    val print_flg = args(4).trim.toBoolean

    // -----------------数据表预删除处理------------------------------------
    val tableList = Array("_xxrr_grad_daily", "_chn_dxrr_grad_daily")
    MoxiuUtil.deleTable(productName, dataDate, tableList, 1)

    val sparkConf = new SparkConf
    sparkConf.setAppName("DimensionCalculate")
    val sc = new SparkContext(sparkConf)

    // 日纬度表： 0:userId,1:ctime,2:版本,3:渠道，4:品牌，5:机型，6:网络，7:分辨率，8:安卓版本，9:省，10:市，11:运营商
    // 留存总表：0:userId,1:startDate,2:lastDate,3:版本,4:渠道，5:品牌，6:机型，7:网络，8:分辨率，9:安卓版本，10:省，11:市,12:运营商
    val liuCunpath = liuCunRootPath + "/" + MoxiuUtil.getDate(dataDate, 0, "yyyy/MM/dd")
    val dimensionPath = dimensionRootPath + "/" + MoxiuUtil.getDate(dataDate, 0, "yyyy/MM/dd")

    // ----------最新纬度表,过滤掉渠道= ""的数据--------------
    val dimensionRDD = DimensionCore.getRDD(sc, dimensionPath).filter(_(3) != "").map(line => (line(0), (line(1), line(3)))).groupByKey.map(user => {
      // 根据时间降序排序取最大时间记录
      val maxDate = (user._2.toArray.sortWith((a, b) => a._1 > b._1))(0)
      // (userId,渠道)
      (user._1, maxDate._2)
    }).persist
    val liucunRDD = DimensionCore.getRDD(sc, liuCunpath).map(array => { array(1) = array(1).split(" ")(0); array(2) = array(2).split(" ")(0); array }).persist

    // --------------------最新留存表数据抽出-----------------------------------------
    // 留存梯度分析 维度表【date,drr_1,drr_2,drr_3,drr_4,drr_5,drr_6,drr_7,drr_14,drr_30】路径
    val liuCunDateList = getLiuCunDateList(dataDate)
    // 根据渠道抽出注册时间为liuCunDateList的新用户
    val xrrUser_rrd = DimensionCore.liuCunFilter(liucunRDD, liuCunDateList, Array(4)).persist
    // --------------------每天的新增用户RDD(userId,date,新增用户渠道) --------------
    //把出【date,drr_1,drr_2,drr_3,drr_4,drr_5,drr_6,drr_7,drr_14,drr_30】每天的新增用户RDD(userId,date,新增用户渠道) 
    val newUser = liuCunDateList.map(date => {
      def setDate(dateStr: String)(array: Array[String]) = (array(0), dateStr, array(4))
      // (userId,日期，渠道)
      xrrUser_rrd.filter(_(1) == date).map(setDate(date)).persist
    })
    xrrUser_rrd.unpersist()
    // --------------------【 launcher_xxrr_grad_daily】-----------------------------
    val xxrrList = (for (index <- 0 until newUser.size) yield {
      val newUserCount = newUser(index).count
      if (index == 0) newUserCount.toString else {
        val lc_Count = (newUser(index).map(_._1)).intersection(dimensionRDD.map(_._1)).count
        if (newUserCount==0) "0" else  (lc_Count / newUserCount.toDouble).toString
      }
    }).toList

    val xxrrArry = ArrayBuffer[String]()
    xxrrArry += "DATE"
    xxrrArry += dataDate
    for (e <- xxrrList) xxrrArry += e
    // --------------------【 launcher_chn_dxrr_grad_daily】-----------------------------
    //DATE_S|CHANNEL|DNU|D1RR|D2RR|D3RR|D4RR|D5RR|D6RR|D7RR|D14RR|D30RR

    val chn_dxrrRdd = (for (index <- 0 until newUser.size) yield {
      def setIndex(index: Int)(kv: (String, String)) = (kv._1, (index, kv._2))
      // 新增用户按[渠道]统计总量
      val newUser_ch_RDD = newUser(index).map(line => (line._3, 1)).reduceByKey(_ + _)
      if (index == 0) newUser_ch_RDD.map(f => (f._1, f._2.toString)).map(setIndex(index))
      else {
        // 留存与日维度表按【渠道】取交集
        val intersection = (newUser(index).map(line => line._1 + separator + line._3)).intersection(dimensionRDD.map(line => line._1 + separator + line._2))
          .map(line => (line.split(separator)(1), 1)).reduceByKey(_ + _)
        // 算比率 XRR
        intersection.leftOuterJoin(newUser_ch_RDD).map(f => {
          val channel = f._1
          val intersectionCount = f._2._1
          val userCount = if (f._2._2 == None) 0 else f._2._2.get
          (channel, (intersectionCount / userCount.toDouble).toString)
        }).map(setIndex(index))
      }
    })
    newUser.map(rdd => rdd.unpersist())

    // (渠道,(index,计算值))  汇总10天的数据
    val unionRDD = chn_dxrrRdd.reduceLeft((rdd1, rdd2) => rdd1.union(rdd2))
    def fun_1(date: String)(line: (String, Map[Int, String])) = {
      val channel = line._1
      val map = line._2
      Array(date, channel, map.getOrElse(0, "0"), map.getOrElse(1, "0"), map.getOrElse(2, "0"), map.getOrElse(3, "0"), map.getOrElse(4, "0"),
        map.getOrElse(5, "0"), map.getOrElse(6, "0"), map.getOrElse(7, "0"), map.getOrElse(8, "0"), map.getOrElse(9, "0"))
    }
    def fun(date: String)(line: (String, Iterable[(Int, String)])) = {
      val channel = line._1
      val map = Map[Int, String]()
      for (e <- line._2) map += e
      Array(date, channel, map.getOrElse(0, "0"), map.getOrElse(1, "0"), map.getOrElse(2, "0"), map.getOrElse(3, "0"), map.getOrElse(4, "0"),
        map.getOrElse(5, "0"), map.getOrElse(6, "0"), map.getOrElse(7, "0"), map.getOrElse(8, "0"), map.getOrElse(9, "0"))
    }
    val dxrr_result = unionRDD.groupByKey.map(fun(dataDate)).collect
    //---------------------------------结果打印现示--------------------------------
    val msg = "-------------[X_xxrr_grad_daily]-----------------------"
    val items = Array("SPAN", "TIME_S", "XNU", "X1RR", "X2RR", "X3RR", "X4RR", "X5RR", "X6RR", "X7RR", "X8RR", "X9RR")
    MoxiuUtil.updatePrint(print_flg, productName, "_xxrr_grad_daily", dataDate, Array(xxrrArry.toArray), items, msg)
    val msg1 = "-------------[X_chn_dxrr_grad_daily]------------------"
    val items_1 = Array("DATE_S", "CHANNEL", "DNU", "X1RR", "X2RR", "X3RR", "X4RR", "X5RR", "X6RR", "X7RR", "X8RR", "X9RR")
    MoxiuUtil.updatePrint(print_flg, productName, "_chn_dxrr_grad_daily", dataDate, dxrr_result, items_1, msg1)
    liucunRDD.unpersist()
    dimensionRDD.unpersist()
    sc.stop
  }

  /**
   * 编历维度文件列表
   * 返回date日期下计算--[活跃、新增用DAU,WAU,MAU]的维度文件列表,以及计算留存用的日期
   */
  def getLiuCunDateList(date: String) = {
    val drr_1 = MoxiuUtil.getDate(date, -1, "yyyy-MM-dd")
    val drr_2 = MoxiuUtil.getDate(date, -2, "yyyy-MM-dd")
    val drr_3 = MoxiuUtil.getDate(date, -3, "yyyy-MM-dd")
    val drr_4 = MoxiuUtil.getDate(date, -4, "yyyy-MM-dd")
    val drr_5 = MoxiuUtil.getDate(date, -5, "yyyy-MM-dd")
    val drr_6 = MoxiuUtil.getDate(date, -6, "yyyy-MM-dd")
    val drr_7 = MoxiuUtil.getDate(date, -7, "yyyy-MM-dd")
    val drr_14 = MoxiuUtil.getDate(date, -14, "yyyy-MM-dd")
    val drr_30 = MoxiuUtil.getDate(date, -30, "yyyy-MM-dd")
    Array(date, drr_1, drr_2, drr_3, drr_4, drr_5, drr_6, drr_7, drr_14, drr_30)
  }
}