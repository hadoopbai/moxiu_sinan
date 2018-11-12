package com.moxiu.shucang.sinan

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting.quickSort
import org.apache.spark.{ SparkConf, SparkContext }
import moxiu.java.{ IpBean, IPLocalInfo }
import scala.io.Source
import moxiu.sinan.LiuCunProperty
import org.apache.spark.rdd.RDD

/**
 * @author 宿荣全
 * @date 2017-07-04
 * 每天抽取用户各维度数据
 */
object LiuCunDayExtract {

  def main(args: Array[String]) {

    val confName = args(0)
    val propBean = (new LiuCunProperty(confName)).getProperties
    val iplib = propBean.getIpFile
    val typeint = propBean.getTypeId
    val (path, output, fieldSize) = (propBean.getDayinput, propBean.getDayoutput, propBean.getFieldSize)
    println("=======================输入数据路径：" + path)
    println("=======================结果存放路径：" + output)
    println("=======================IP映射表路径：" + iplib)

    // 初始化ip列表
    val treeMap = IPLocalInfo.loadIpLib(iplib)

    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName("LiuCunDayExtract")

    val separator = "\001"
    var rdd_filter: RDD[Array[String]] = null
    // 过滤掉大于等于100个字符的下载渠道
    val rdd = sc.textFile(path).map(_.split(separator)).filter(lineList => (lineList.size >= fieldSize && lineList(14).length < 100))

    // 微锁屏用户最从vcode是100以上的  注：此处过滤只适应vcode是3位的
    rdd_filter = if ("2".equals(typeint)) rdd.filter(lineList => lineList(18).trim.matches("[1-9][0-9]+") && lineList(18).toInt > 100) else rdd

    rdd_filter.map(itemList => {

      val userId = itemList(0)
      val ctime = itemList(1)
      val model = itemList(6)
      val manufacturer = itemList(7)
      val display = itemList(10)
      val net = itemList(11)
      val ver = itemList(13)
      val chanel = itemList(14)
      val androidsdk = itemList(19)
      val ip = itemList(2)
      // 省，市,运营商
      val localInfo = IPLocalInfo.getLocal(ip, treeMap)

      // userId，ctime,版本,渠道，品牌，机型，网络，分辨率，安卓版本，[运营商,区域(省市)]
      val array = Array[String](userId, "", ver, chanel, manufacturer, model, net, display, androidsdk)
      val unArray = array ++ localInfo
      (unArray.mkString(separator), ctime)
    }).groupByKey.map(list => {
      val array = list._1.split(separator)
      // 同维度取最小时间
      val firstTime = list._2.toList.min
      array(1) = firstTime
      // userId，ctime,版本,渠道，品牌，机型，网络，分辨率，安卓版本，省，市，运营商
      array.mkString(separator)
    }).repartition(1).saveAsTextFile(output)
    sc.stop
  }
}