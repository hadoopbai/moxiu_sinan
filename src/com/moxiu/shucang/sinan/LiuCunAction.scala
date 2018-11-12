package com.moxiu.shucang.sinan

import scala.util.Sorting.quickSort
import org.apache.spark.{ SparkConf, SparkContext }
import moxiu.java.IPLocalInfo
import moxiu.sinan.LiuCunProperty
import org.apache.spark.rdd.RDD

/**
 * 留存数据抽取
 * @author 宿荣全
 */
object LiuCunAction {

  def main(args: Array[String]) {

    val confName = args(0)
    val propBean = (new LiuCunProperty(confName)).getProperties

    val iplib = propBean.getIpFile
    val typeint = propBean.getTypeId
    val (path, output, mix_FieldSize) = (propBean.getLc_input, propBean.getLc_output, propBean.getLc_minFieldSize)
    println("=======================抽取清洗表路径：" + path)
    println("=======================留存结果存放路径：" + output)
    println("=======================IP映射表路径：" + iplib)

    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName("SubLiuCun")

    val separator = "\001"

    // 初始化ip列表
    val treeMap = IPLocalInfo.loadIpLib(iplib)

    var rdd_filter: RDD[Array[String]] = null
    // 过滤掉大于等于100个字符的下载渠道
    val rdd = sc.textFile(path).map(_.split(separator)).filter(lineList => lineList.size > mix_FieldSize && lineList(14).length < 100)
    // 微锁屏用户最从vcode是100以上的 注：此处过滤只适应vcode是3位的
    rdd_filter = if ("2".equals(typeint)) rdd.filter(lineList => lineList(18).trim.matches("[1-9][0-9]+") && lineList(18).toInt > 100) else rdd

    rdd_filter.map(list => {
      val userId = list(0)
      val ctime = list(1)
      val model = list(6)
      val manufacturer = list(7)
      val display = list(10)
      val net = list(11)
      val ver = list(13)
      val chanel = list(14)
      val androidsdk = list(19)
      val ip = list(2)

      // userId，[startDate,lastDate],版本,渠道，品牌，机型，网络，分辨率，安卓版本，[运营商,区域(省市)]
      val array = Array(userId, ctime, ctime, ver, chanel, manufacturer, model, net, display, androidsdk, ip)
      (userId, (ctime, array))
    }).filter(line => (!"".equals(line._1.trim) && (line._2._1.trim).size > 9)).groupByKey.map(lineList => {
      val list = lineList._2.toArray
      // 降序
      quickSort(list)(Ordering.by[(String, Array[String]), String](_._1))
      // 取最小时间记录
      val lineArray = list(0)._2
      val lastData = (list(list.size - 1)._2)(2)
      lineArray(2) = lastData

      val ip = lineArray(10)
      // 省，市,运营商
      val localInfo = IPLocalInfo.getLocal(ip, treeMap)
      (lineArray.take(10) ++ localInfo).mkString(separator)
    }).repartition(100).saveAsTextFile(output)
    sc.stop
  }

  //userId，startDate,lastDate,版本,渠道,区域(省市)，品牌，机型，网络，运营商，分辨率，安卓版本
  // muuid,ctime,ip,ua,androidid,imei,model,manufacturer,locale,mac,display,net,ipaddr,ver,child,homepackage,install_tamp,time_tamp,vcode,androidsdk,content_timestamp,act,type,osbuild,osbuildshanzai,ic,ts

  //0	 muuid	1000727b630c9c16|867809023068741
  //1	ctime	2017/6/26 18:18
  //2	ip	113.200.106.157
  //3	ua	Dalvik/1.6.0 (Linux; U; Android 4.4.4; Coolpad 8721 Build/KTU84P)
  //4	androidid	1000727b630c9c16
  //5	imei	8.67809E+14
  //6	model	Coolpad 8721
  //7	manufacturer	Yulong
  //8	locale	zh_CN
  //9	mac	f6:d0:10:ef:fb:08
  //10	display	720*1280
  //11	net	twoGNetStatus
  //12	ipaddr	10.39.65.250
  //13	ver	3.7.1
  //14	child	A-baidu
  //15	homepackage	null
  //16	install_tamp	1970/1/1 8:00
  //17	time_tamp	2017/6/26 18:17
  //18	vcode	371
  //19	androidsdk	19
  //20	content_timestamp	2017/6/26 18:17
  //21	act	active
  //22	type	base
  //23	osbuild	null
  //24	osbuildshanzai	null
  //25	ic	null
  //26	ts	null
}