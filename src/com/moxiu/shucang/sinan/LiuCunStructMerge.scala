package com.moxiu.shucang.sinan

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting.quickSort
import org.apache.spark.{ SparkConf, SparkContext }
import moxiu.sinan.LiuCunProperty
import org.apache.spark.rdd.RDD

object LiuCunStructMerge {
  def main(args: Array[String]) {

    val confName = args(0)
    val propBean = (new LiuCunProperty(confName)).getProperties
    val typeint = propBean.getTypeId
    val (path, output) = (propBean.getMergeinput, propBean.getMergeoutput)

    println("=======================用户留存初始数据源表路径：" + path)
    println("===========================合并留存结果存放路径：" + output)
    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName("DayLiuCunMeger")

    val separator = "\001"
    // userId，[startDate,lastDate],版本,渠道，品牌，机型，网络，分辨率，安卓版本，省，市,运营商
    // val array = ArrayBuffer(userId, ctime, ctime, ver, chanel, manufacturer, model, net, display, androidsdk, 省，city,net)

    // 支持多路径合并
    val pathList = path.split(",")
    val rdd = if (pathList.size > 1) {
      val rddList = for (p <- pathList) yield sc.textFile(path)
      rddList.reduce((a, b) => a.union(b))
    } else sc.textFile(path)

    rdd.map(line => {

      val itemList = line.split(separator)

      val userid = itemList(0)
      val startDate = itemList(1)
      val lastDate = itemList(2)
      (userid, (startDate, lastDate, line))
    }).groupByKey.map(groupList => {
      val lineList = groupList._2.toArray

      // 新增用户
      if (lineList.size == 1) lineList(0)._3 else {
        // 留存用户
        // 按lastDate 升序排序
        quickSort(lineList)(Ordering[(String)].on(x => x._2))
        // 获取最新日期
        val lastDate = (lineList(lineList.size - 1))._2

        // 按startDate 升序排序
        quickSort(lineList)(Ordering[(String)].on(x => x._1))
        //获取最早的按装信息
        val minline = lineList(0)._3
        val list = minline.split(separator).toArray
        // 修改最近访问时间
        list(2) = lastDate
        list.mkString(separator)
      }
    }).repartition(100).saveAsTextFile(output)
    sc.stop
  }
}