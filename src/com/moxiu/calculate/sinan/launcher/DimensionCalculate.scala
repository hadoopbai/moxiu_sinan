package com.moxiu.calculate.sinan.launcher

import org.apache.spark.{ SparkConf, SparkContext }
import com.moxiu.shucang.sinan.MoxiuUtil
import moxiu.sinan.DimensionCore
import scala.collection.mutable.ArrayBuffer
import com.moxiu.db.mysql.ExecutSql
import org.apache.spark.rdd.RDD

/**
 * @author 宿荣全
 * @comment 司南平台的维度计算
 * <br>纬度包括：版本+渠道、版本+渠道+省+市+运营商</br>
 *  <br>日纬度表： userId,ctime,版本,渠道，品牌，机型，网络，分辨率，安卓版本，省，市，运营商 </br>
 *  <br>留存总表：userId,startDate,lastDate,3:版本,4:渠道，5:品牌，6:机型，7:网络，8:分辨率，9:安卓版本，10:省，11:市,12:运营商</br>
 * @date 2017-08-06
 */
object DimensionCalculate {

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

    val tableList = Array("_ver_daily",
      "_channel_daily",
      "_channel_ver_daily",
      "_host_channel_ver_daily",
      "_host_channel_daily",
      "_host_ver_daily",
      "_host_daily",
      "_isp_channel_ver_daily",
      "_isp_channel_daily",
      "_isp_ver_daily",
      "_isp_daily",
      "_net_channel_daily",
      "_net_channel_ver_daily",
      "_net_daily",
      "_net_ver_daily",
      "_device_channel_daily",
      "_device_channel_ver_daily",
      "_device_daily",
      "_device_ver_daily")
    // 数据表预删除处理
    MoxiuUtil.deleTable(productName, dataDate, tableList, 1)

    // 日纬度表： 0:userId,1:ctime,2:版本,3:渠道，4:品牌，5:机型，6:网络，7:分辨率，8:安卓版本，9:省，10:市，11:运营商
    // 留存总表：0:userId,1:startDate,2:lastDate,3:版本,4:渠道，5:品牌，6:机型，7:网络，8:分辨率，9:安卓版本，10:省，11:市,12:运营商
    val liuCunpath = liuCunRootPath + "/" + MoxiuUtil.getDate(dataDate, 0, "yyyy/MM/dd")
    val dimensionPath = dimensionRootPath + "/" + MoxiuUtil.getDate(dataDate, 0, "yyyy/MM/dd")

    val sparkConf = new SparkConf
    sparkConf.setAppName("DimensionCalculate")
    val sc = new SparkContext(sparkConf)

    // 最新留存表(截取start,end的年月日部分)
    val liucunRDD = DimensionCore.getRDD(sc, liuCunpath).map(array => {array(1) = array(1).split(" ")(0);array(2) = array(2).split(" ")(0);array}).persist

    // 过滤掉MAU为1的数据，存在解析错行数据
    def filterRDD(rdd: RDD[Array[String]], index: Int) = rdd.filter(fun(index))
    def fun(index: Int)(array: Array[String]) = if (array(index).toInt > 1) true else false
    // dAU小于200条的不计算(产品设定的数值)
    def filter_Dau_RDD(rdd: RDD[(String, String)]) = rdd.filter(_._2.toInt > 200)
    // 版本过滤：9.8.8.1 最多11位过滤IP，数字开头，必须带点
    def verFilter(index: Int)(array: Array[String]) = if (!(array(index).size > 11) && array(index).matches("^[0-9].*") && array(index).matches(".*\\..*")) true else false
    // 渠道过滤：过滤，数字和点的组合（IP，版本）
    def channelFilter(index: Int)(array: Array[String]) = if (!array(index).matches("[0-9|\\.]+")) true else false
    //--------------------------版本\渠道计算 留存用户、新增用户、活跃用户计算--------------------------------
    // DATE|VER|CHANNEL|DAU|WAU|MAU|DNU|WNU|MNU|DRR|WRR|MRR --------[launcher_channel_ver_daily]
    val channel_ver_rdd = DimensionCore.dimensionCalculate(sc, dimensionRootPath, dimensionPath, liucunRDD, dataDate, Array(2, 3), Array(3, 4))
    val launcher_channel_ver_daily_Items = Array("DATE", "VER", "CHANNEL", "DAU", "WAU", "MAU", "DNU", "WNU", "MNU", "DRR", "WRR", "MRR")
    val channel_ver_fiter = filterRDD(channel_ver_rdd, 5)
    val channel_ver_result = DimensionCore.updateCollect(channel_ver_fiter, productName, "_channel_ver_daily", dataDate, launcher_channel_ver_daily_Items)

    // [DATE|VER|DAU|WAU|MAU|DNU|WNU|MNU|DRR|WRR|MRR]--------[launcher_ver_daily]
    val ver_rdd = DimensionCore.dimensionCalculate(sc, dimensionRootPath, dimensionPath, liucunRDD, dataDate, Array(2), Array(3))
    val ver_daily_Items = Array("DATE", "VER", "DAU", "WAU", "MAU", "DNU", "WNU", "MNU", "DRR", "WRR", "MRR")
    val ver_fiter = filterRDD(ver_rdd, 4).filter(verFilter(1))
    val ver_result = DimensionCore.updateCollect(ver_fiter, productName, "_ver_daily", dataDate, ver_daily_Items)

    //  [DATE|CHANNEL|DNU|WNU|MNU|DAU|WAU|MAU|DRR|WRR|MRR]--------[launcher_channel_daily]
    val channel_rdd = DimensionCore.dimensionCalculate(sc, dimensionRootPath, dimensionPath, liucunRDD, dataDate, Array(3), Array(4))
    val channel_daily_Items = Array("DATE", "CHANNEL", "DAU", "WAU", "MAU", "DNU", "WNU", "MNU", "DRR", "WRR", "MRR")
    val channel_fiter = filterRDD(channel_rdd, 4).filter(channelFilter(1)).filter(data=>data(4).toInt > 200)
    val channel_result = DimensionCore.updateCollect(channel_fiter, productName, "_channel_daily", dataDate, channel_daily_Items)
    
    //---------------------------------地域分布计算-------------------------------------------
    // 日纬度表： 0:userId,1:ctime,2:版本,3:渠道，4:品牌，5:机型，6:网络，7:分辨率，8:安卓版本，9:省，10:市，11:运营商
    // 留存总表：0:userId,1:startDate,2:lastDate,3:版本,4:渠道，5:品牌，6:机型，7:网络，8:分辨率，9:安卓版本，10:省，11:市,12:运营商
    val weidu_rdd = DimensionCore.getRDD(sc, dimensionPath).persist

    // DATE|CHANNEL|VER|CITY|PROVINCE|DNU|DAU  --------[launcher_host_channel_ver_daily]
    val channel_ver_city_province_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 2, 10, 9))
    val channel_ver_city_province_filter = filter_Dau_RDD(channel_ver_city_province_DAU)
    val channel_ver_city_province_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 3, 11, 10))
    val channel_ver_city_province_rdd = DimensionCore.dauJoinDnu(dataDate, channel_ver_city_province_filter, channel_ver_city_province_DNU)
    val channel_ver_city_province_DAU_DNU_items = Array("DATE", "CHANNEL", "VER", "CITY", "PROVINCE", "DNU", "DAU")
    val channel_ver_city_province_DAU_DNU = DimensionCore.updateCollect(channel_ver_city_province_rdd, productName, "_host_channel_ver_daily", dataDate, channel_ver_city_province_DAU_DNU_items)

    // DATE|CHANNEL|CITY|PROVINCE| DNU| DAU| ---------------[launcher_host_channel_daily]
    val channel_city_province_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 10, 9))
    val channel_city_province_filter = filter_Dau_RDD(channel_city_province_DAU)
    val channel_city_province_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 11, 10))
    val channel_city_province_rdd = DimensionCore.dauJoinDnu(dataDate, channel_city_province_filter, channel_city_province_DNU)
    val channel_city_province_DAU_DNU_items = Array("DATE", "CHANNEL", "CITY", "PROVINCE", "DNU", "DAU")
    val channel_city_province_DAU_DNU = DimensionCore.updateCollect(channel_city_province_rdd, productName, "_host_channel_daily", dataDate, channel_city_province_DAU_DNU_items)

    // DATE|VER|CITY|PROVINCE|DNU|DAU---------------------[launcher_host_ver_daily]
    val ver_city_province_DAU = DimensionCore.mx_AU(weidu_rdd, Array(2, 10, 9))
    val ver_city_province_filter = filter_Dau_RDD(ver_city_province_DAU)
    val ver_city_province_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(3, 11, 10))
    val ver_city_province_rdd = DimensionCore.dauJoinDnu(dataDate, ver_city_province_filter, ver_city_province_DNU)
    val ver_city_province_DAU_DNU_items = Array("DATE", "VER", "CITY", "PROVINCE", "DNU", "DAU")
    val ver_city_province_DAU_DNU = DimensionCore.updateCollect(ver_city_province_rdd, productName, "_host_ver_daily", dataDate, ver_city_province_DAU_DNU_items)

    // DATE|CITY|PROVINCE|DNU|DAU--------------------------------------[launcher_host_daily]
    val city_province_DAU = DimensionCore.mx_AU(weidu_rdd, Array(10, 9))
    val city_province_filter = filter_Dau_RDD(city_province_DAU)
    val city_province_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(11, 10))
    val city_province_rdd = DimensionCore.dauJoinDnu(dataDate, city_province_filter, city_province_DNU)
    val city_province_DAU_DNU_items = Array("DATE", "CITY", "PROVINCE", "DNU", "DAU")
    val city_province_DAU_DNU = DimensionCore.updateCollect(city_province_rdd, productName, "_host_daily", dataDate, city_province_DAU_DNU_items)

    //---------------------------------运营商分布计算-------------------------------------------
    //DATE| CHANNEL|VER|ISP|DNU|DAU-----------------[launcher_isp_channel_ver_daily]    
    val channel_ver_isp_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 2, 11))
    val channel_ver_isp_filter = filter_Dau_RDD(channel_ver_isp_DAU)
    val channel_ver_isp_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 3, 12))
    val channel_ver_isp_rdd = DimensionCore.dauJoinDnu(dataDate, channel_ver_isp_filter, channel_ver_isp_DNU)
    val channel_ver_isp_DAU_DNU_items = Array("DATE", "CHANNEL", "VER", "ISP", "DNU", "DAU")
    val channel_ver_isp_DAU_DNU = DimensionCore.updateCollect(channel_ver_isp_rdd, productName, "_isp_channel_ver_daily", dataDate, channel_ver_isp_DAU_DNU_items)

    //DATE| CHANNEL| ISP| DNU| DAU--------------------[launcher_isp_channel_daily]        
    val channel_isp_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 11))
    val channel_isp_filter = filter_Dau_RDD(channel_isp_DAU)
    val channel_isp_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 12))
    val channel_isp_rdd = DimensionCore.dauJoinDnu(dataDate, channel_isp_filter, channel_isp_DNU)
    val channel_isp_DAU_DNU_items = Array("DATE", "CHANNEL", "ISP", "DNU", "DAU")
    val channel_isp_DAU_DNU = DimensionCore.updateCollect(channel_isp_rdd, productName, "_isp_channel_daily", dataDate, channel_isp_DAU_DNU_items)

    //DATE| VER|ISP| DNU|DAU---------------------------[launcher_isp_ver_daily]           
    val ver_isp_DAU = DimensionCore.mx_AU(weidu_rdd, Array(2, 11))
    val ver_isp_filter = filter_Dau_RDD(ver_isp_DAU)
    val ver_isp_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(3, 12))
    val ver_isp_rdd = DimensionCore.dauJoinDnu(dataDate, ver_isp_filter, ver_isp_DNU)
    val ver_isp_DAU_DNU_items = Array("DATE", "VER", "ISP", "DNU", "DAU")
    val ver_isp_DAU_DNU = DimensionCore.updateCollect(ver_isp_rdd, productName, "_isp_ver_daily", dataDate, ver_isp_DAU_DNU_items)

    //DATE| ISP| DNU|DAU--------------------------------[launcher_isp_daily]                
    val isp_DAU = DimensionCore.mx_AU(weidu_rdd, Array(11))
    val isp_filter = filter_Dau_RDD(isp_DAU)
    val isp_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(12))
    val isp_DAU_rdd = DimensionCore.dauJoinDnu(dataDate, isp_filter, isp_DNU)
    val isp_DAU_DNU_items = Array("DATE", "ISP", "DNU", "DAU")
    val isp_DAU_DNU = DimensionCore.updateCollect(isp_DAU_rdd, productName, "_isp_daily", dataDate, isp_DAU_DNU_items)

    //---------------------------------网络分布计算-------------------------------------------
    // DATE|CHANNEL| NET|DNU|DAU -------------------- [launcher_net_channel_daily]
    val channel_net_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 6))
    val channel_net_filter = filter_Dau_RDD(channel_net_DAU)
    val channel_net_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 7))
    val channel_net_rdd = DimensionCore.dauJoinDnu(dataDate, channel_net_filter, channel_net_DNU)
    val channel_net_DAU_DNU_items = Array("DATE", "CHANNEL", "NET", "DNU", "DAU")
    val channel_net_DAU_DNU = DimensionCore.updateCollect(channel_net_rdd, productName, "_net_channel_daily", dataDate, channel_net_DAU_DNU_items)

    // DATE|CHANNEL|VER| NET| DNU  | DAU  -------------------- [launcher_net_channel_ver_daily]
    val channel_ver_net_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 2, 6))
    val channel_ver_net_filter = filter_Dau_RDD(channel_ver_net_DAU)
    val channel_ver_net_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 3, 7))
    val channel_ver_net_rdd = DimensionCore.dauJoinDnu(dataDate, channel_ver_net_filter, channel_ver_net_DNU)
    val channel_ver_net_DAU_DNU_items = Array("DATE", "CHANNEL", "VER", "NET", "DNU", "DAU")
    val channel_ver_net_DAU_DNU = DimensionCore.updateCollect(channel_ver_net_rdd, productName, "_net_channel_ver_daily", dataDate, channel_ver_net_DAU_DNU_items)

    // DATE|NET| DNU|DAU --------------------[launcher_net_daily]
    val net_DAU = DimensionCore.mx_AU(weidu_rdd, Array(6))
    val net_filter = filter_Dau_RDD(net_DAU)
    val net_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(7))
    val net_rdd = DimensionCore.dauJoinDnu(dataDate, net_filter, net_DNU)
    val net_DAU_DNU_items = Array("DATE", "NET", "DNU", "DAU")
    val net_DAU_DNU = DimensionCore.updateCollect(net_rdd, productName, "_net_daily", dataDate, net_DAU_DNU_items)

    // DATE|VER|NET| DNU | DAU  --------------------[launcher_net_ver_daily]
    val ver_net_DAU = DimensionCore.mx_AU(weidu_rdd, Array(2, 6))
    val ver_net_filter = filter_Dau_RDD(ver_net_DAU)
    val ver_net_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(3, 7))
    val ver_net_rdd = DimensionCore.dauJoinDnu(dataDate, ver_net_filter, ver_net_DNU)
    val ver_net_DAU_DNU_items = Array("DATE", "VER", "NET", "DNU", "DAU")
    val ver_net_DAU_DNU = DimensionCore.updateCollect(ver_net_rdd, productName, "_net_ver_daily", dataDate, ver_net_DAU_DNU_items)

    //---------------------------------设备机型分布计算-------------------------------------------
    // [ launcher_device_channel_daily]  device(机型+品牌+分辨)
    val device_channel_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 5, 4, 7))
    val device_channel_filter = filter_Dau_RDD(device_channel_DAU)
    val device_channel_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 6, 5, 8))
    val device_channel_rdd = DimensionCore.dauJoinDnu(dataDate, device_channel_filter, device_channel_DNU)
    val device_channel_DAU_DNU_items = Array("DATE", "CHANNEL", "MODEL", "BRAND", "DPI", "DNU", "DAU")
    val device_channel_DAU_DNU = DimensionCore.updateCollect(device_channel_rdd, productName, "_device_channel_daily", dataDate, device_channel_DAU_DNU_items)

    // [launcher_device_channel_ver_daily]
    val device_channel_ver_DAU = DimensionCore.mx_AU(weidu_rdd, Array(3, 2, 5, 4, 7))
    val device_channel_ver_filter = filter_Dau_RDD(device_channel_ver_DAU)
    val device_channel_ver_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(4, 3, 6, 5, 8))
    val device_channel_ver_rdd = DimensionCore.dauJoinDnu(dataDate, device_channel_ver_filter, device_channel_ver_DNU)
    val device_channel_ver_DAU_DNU_items = Array("DATE", "CHANNEL", "VER", "MODEL", "BRAND", "DPI", "DNU", "DAU")
    val device_channel_ver_DAU_DNU = DimensionCore.updateCollect(device_channel_ver_rdd, productName, "_device_channel_ver_daily", dataDate, device_channel_ver_DAU_DNU_items)

    // [launcher_device_daily]  维度："MODEL", "BRAND", "DPI"
    val device_DAU = DimensionCore.mx_AU(weidu_rdd, Array(5, 4, 7))
    val device_filter = filter_Dau_RDD(device_DAU)
    val device_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(6, 5, 8))
    val dimensionIndexs = Array(5, 4, 7)
    val liuCunIndexs = Array(6, 5, 8)
    val drrDate = Array(MoxiuUtil.getDate(dataDate, -1, "yyyy-MM-dd"))
    val device_DRR = (DimensionCore.xRR_Calculate(sc, dimensionPath, liucunRDD, drrDate, dimensionIndexs, liuCunIndexs))(0)
    val device_DAU_DNU_items = Array("date", "MODEL", "BRAND", "DPI", "DNU", "DAU", "DRR")
    val device_DAU_DNU_rdd = device_filter.leftOuterJoin(device_DNU).leftOuterJoin(device_DRR).map(DimensionCore.dAuJoinNuJoinDRR(dataDate))
    val device_DAU_DNU = DimensionCore.updateCollect(device_DAU_DNU_rdd, productName, "_device_daily", dataDate, device_DAU_DNU_items)

    // [launcher_device_ver_daily]
    val device_ver_DAU = DimensionCore.mx_AU(weidu_rdd, Array(2, 5, 4, 7))
    val device_ver_filter = filter_Dau_RDD(device_ver_DAU)
    val device_ver_DNU = DimensionCore.mx_xNU(liucunRDD, dataDate, dataDate, Array(3, 6, 5, 8))
    val device_ver_rdd = DimensionCore.dauJoinDnu(dataDate, device_ver_filter, device_ver_DNU)
    val device_ver_DAU_DNU_items = Array("DATE", "VER", "MODEL", "BRAND", "DPI", "DNU", "DAU")
    val device_ver_DAU_DNU = DimensionCore.updateCollect(device_ver_rdd, productName, "_device_ver_daily", dataDate, device_ver_DAU_DNU_items)

    weidu_rdd.unpersist()
    liucunRDD.unpersist()

    sc.stop
    //---------------------------------结果打印现示-------------------------------------------
    val msg_A = "------------------留存用户、新增用户、活跃用户------------------"
    val msg1 = "channel_ver_result-------------[launcher_channel_ver_daily]-----------------------"
    MoxiuUtil.outPrint(print_flg, channel_ver_result, launcher_channel_ver_daily_Items, msg_A, msg1)
    val msg1_1 = "ver_result-------------[launcher_host_channel_ver_daily]-----------------------"
    MoxiuUtil.outPrint(print_flg, ver_result, ver_daily_Items, msg1_1)
    val msg1_2 = "channel_result-------------[launcher_host_channel_ver_daily]------------------"
    MoxiuUtil.outPrint(print_flg, channel_result, channel_daily_Items, msg1_2)
    val msg_B = "------------------地域分布计算 DAU、DNU------------------"
    val msg2 = "channel_ver_city_province_DAU_DNU-------------[launcher_host_channel_ver_daily]-----------------------"
    MoxiuUtil.outPrint(print_flg, channel_ver_city_province_DAU_DNU, channel_ver_city_province_DAU_DNU_items, msg_B, msg2)
    val msg3 = "channel_city_province_DAU_DNU-----------------[launcher_host_channel_daily]---------------------------"
    MoxiuUtil.outPrint(print_flg, channel_city_province_DAU_DNU, channel_city_province_DAU_DNU_items, msg3)
    val msg4 = "ver_city_province_DAU_DNU----------------------[launcher_host_ver_daily]--------------------------------"
    MoxiuUtil.outPrint(print_flg, ver_city_province_DAU_DNU, ver_city_province_DAU_DNU_items, msg4)
    val msg5 = "city_province_DAU_DNU--------------------------[launcher_host_daily]------------------------------------"
    MoxiuUtil.outPrint(print_flg, city_province_DAU_DNU, city_province_DAU_DNU_items, msg5)
    val msg_C = "------------------运营商分布计算 DAU、DNU---------------"
    val msg6 = "channel_ver_isp_DAU_DNU-----------------------[launcher_isp_channel_ver_daily]   -----------------------"
    MoxiuUtil.outPrint(print_flg, channel_ver_isp_DAU_DNU, channel_ver_isp_DAU_DNU_items, msg_C, msg6)
    val msg7 = "channel_isp_DAU_DNU---------------------------[launcher_isp_channel_daily] ----------------------------"
    MoxiuUtil.outPrint(print_flg, channel_isp_DAU_DNU, channel_isp_DAU_DNU_items, msg7)
    val msg8 = "ver_isp_DAU_DNU--------------------------------[launcher_isp_ver_daily] ---------------------------------"
    MoxiuUtil.outPrint(print_flg, ver_isp_DAU_DNU, ver_isp_DAU_DNU_items, msg8)
    val msg9 = "isp_DAU_DNU------------------------------------[launcher_isp_daily]-------------------------------------"
    MoxiuUtil.outPrint(print_flg, isp_DAU_DNU, isp_DAU_DNU_items, msg9)
    val msg_D = "------------------网络分布计算 DAU、DNU-----------------"
    val msg10 = "channel_net_DAU_DNU--------------------------[launcher_net_channel_daily]-----------------------------"
    MoxiuUtil.outPrint(print_flg, channel_net_DAU_DNU, channel_net_DAU_DNU_items, msg_D, msg10)
    val msg11 = "channel_ver_net_DAU_DNU----------------------[launcher_net_channel_ver_daily]-------------------------"
    MoxiuUtil.outPrint(print_flg, channel_ver_net_DAU_DNU, channel_ver_net_DAU_DNU_items, msg11)
    val msg12 = "net_DAU_DNU-----------------------------------[launcher_net_daily]-------------------------------------"
    MoxiuUtil.outPrint(print_flg, net_DAU_DNU, net_DAU_DNU_items, msg12)
    val msg13 = "ver_net_DAU_DNU-------------------------------[launcher_net_ver_daily]---------------------------------"
    MoxiuUtil.outPrint(print_flg, ver_net_DAU_DNU, ver_net_DAU_DNU_items, msg13)
    val msg_E = "------------------设备机型分布计算 DAU、DNU-----------------"
    val msg14 = "device_channel_DAU_DNU----------------------[ launcher_device_channel_daily]-------------------------"
    MoxiuUtil.outPrint(print_flg, device_channel_DAU_DNU, device_channel_DAU_DNU_items, msg_E, msg14)
    val msg15 = "device_channel_ver_DAU_DNU------------------[launcher_device_channel_ver_daily]----------------------"
    MoxiuUtil.outPrint(print_flg, device_channel_ver_DAU_DNU, device_channel_ver_DAU_DNU_items, msg15)
    val msg16 = "device_DAU_DNU------------------------------[launcher_device_daily]-----------------------------------"
    MoxiuUtil.outPrint(print_flg, device_DAU_DNU, device_DAU_DNU_items, msg16)
    val msg17 = "device_ver_DAU_DNU--------------------------[launcher_device_ver_daily]-------------------------------"
    MoxiuUtil.outPrint(print_flg, device_ver_DAU_DNU, device_ver_DAU_DNU_items, msg17)
    
    // ==关于维度计算共计更新19个表=========
    //    delete from  launcher_ver_daily;
    //    delete from launcher_channel_daily;
    //    delete from launcher_channel_ver_daily;
    //    delete from launcher_host_channel_ver_daily;
    //    delete from launcher_host_channel_daily;
    //    delete from launcher_host_ver_daily;
    //    delete from launcher_host_daily;
    //    delete from launcher_isp_channel_ver_daily;
    //    delete from launcher_isp_channel_daily;
    //    delete from launcher_isp_ver_daily;
    //    delete from launcher_isp_daily;
    //    delete from launcher_net_channel_daily;
    //    delete from launcher_net_channel_ver_daily;
    //    delete from launcher_net_daily;
    //    delete from launcher_net_ver_daily;
    //    delete from launcher_device_channel_daily;
    //    delete from launcher_device_channel_ver_daily;
    //    delete from launcher_device_ver_daily;
    //    delete from launcher_device_daily;
    //----------------------------------------------
    //    select count(*) from  launcher_ver_daily;
    //    select count(*) from launcher_channel_daily;
    //    select count(*) from launcher_channel_ver_daily;
    //    select count(*) from launcher_host_channel_ver_daily;
    //    select count(*) from launcher_host_channel_daily;
    //    select count(*) from launcher_host_ver_daily;
    //    select count(*) from launcher_host_daily;
    //    select count(*) from launcher_isp_channel_ver_daily;
    //    select count(*) from launcher_isp_channel_daily;
    //    select count(*) from launcher_isp_ver_daily;
    //    select count(*) from launcher_isp_daily;
    //    select count(*) from launcher_net_channel_daily;
    //    select count(*) from launcher_net_channel_ver_daily;
    //    select count(*) from launcher_net_daily;
    //    select count(*) from launcher_net_ver_daily;
    //    select count(*) from launcher_device_channel_daily;
    //    select count(*) from launcher_device_channel_ver_daily;
    //    select count(*) from launcher_device_ver_daily;
  }
}