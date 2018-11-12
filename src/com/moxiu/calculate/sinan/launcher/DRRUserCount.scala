package com.moxiu.calculate.sinan.launcher

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import com.moxiu.shucang.sinan.MoxiuUtil

/**
 * 通过留存汇总表计算桌面|微锁屏用户的【日活】、【周活】、【月活】、【日新】、【周新】、【月新】 。
 * 桌面、微锁屏交集 DAU、MAU、	total_DNU
 * @author 宿荣全
 * @date 2017.07.11
 * date:所跑数据当天日期
 */
object DRRUserCount {

  def main(args: Array[String]): Unit = {

    val launcherRoot = args(0)
    val vlockRoot = args(1)
    val date = args(2).trim
    // 是否打印表数据到文件
    val print_flg = args(3).trim.toBoolean
    // 留存目录 
    val launcherPath = launcherRoot + "/" + MoxiuUtil.getDate(date, 0, "yyyy/MM/dd")
    val vlockPath = vlockRoot + "/" + MoxiuUtil.getDate(date, 0, "yyyy/MM/dd")

    val d1rr = MoxiuUtil.getDate(date, -1, "yyyy-MM-dd")
    val d7rr = MoxiuUtil.getDate(date, -7, "yyyy-MM-dd")
    val d30rr = MoxiuUtil.getDate(date, -30, "yyyy-MM-dd")

    val d7 = MoxiuUtil.getDate(date, -6, "yyyy-MM-dd")
    val d30 = MoxiuUtil.getDate(date, -29, "yyyy-MM-dd")

    // -----------------数据表预删除处理------------------------------------
    val vlock_tableList = Array("vlock_summary_daily")
    MoxiuUtil.deleTable("mcmp_vlock", date, vlock_tableList, 2)
    val launcher_tableList = Array("launcher_summary_daily", "launcher_vlock_union")
    MoxiuUtil.deleTable("mcmp_launcher", date, launcher_tableList, 2)

    val separator = "\001"
    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName("DRRUserCount")

    // userId，[startDate,lastDate],版本,渠道，品牌，机型，网络，分辨率，安卓版本，省，市,运营商
    def getUserDate(line: String) = {
      val infoList = line.split(separator)
      val user = infoList(0)
      val start = infoList(1).split(" ")(0)
      val end = infoList(2).split(" ")(0)
      (user, (user, start, end))
    }
    // -----------------桌面用户[新增、活跃用户]计算------------------------------------
    // table launcher_summary_daily: [DATE,DNU,WNU,MNU,DAU,WAU,MAU,AAU,DRR,WRR,MRR]
    val launcherRdd = sc.textFile(launcherPath).map(getUserDate).persist
    val launcherDNU = getXNU(launcherRdd, date).persist
    val launcherWNU = getXNU(launcherRdd, d7)
    val launcherMNU = getXNU(launcherRdd, d30)
    val launcherDAU = getXAU(launcherRdd, date).persist
    val launcherWAU = getXAU(launcherRdd, d7)
    val launcherMAU = getXAU(launcherRdd, d30).persist
    val launcherDRR = getXRR(launcherRdd, d1rr, date)
    val launcherWRR = getXRR(launcherRdd, d7rr, date)
    val launcherMRR = getXRR(launcherRdd, d30rr, date)

    val launcherAccumlateCount = launcherRdd.count
    val l_DNU_count = launcherDNU.count
    val l_WNU_count = launcherWNU.count
    val l_MNU_count = launcherMNU.count
    val l_DAU_count = launcherDAU.count
    val l_WAU_count = launcherWAU.count
    val l_MAU_count = launcherMAU.count
    val l_DRR_count = launcherDRR.count
    val l_WRR_count = launcherWRR.count
    val l_MRR_count = launcherMRR.count
    val launcherArray = Array(date, l_DNU_count, l_WNU_count, l_MNU_count, l_DAU_count, l_WAU_count, l_MAU_count, launcherAccumlateCount)

    // 留存比率记算
    val launcherDNU_1 = getXNU_RDD(launcherRdd, d1rr)
    val launcherDWU_7 = getXNU_RDD(launcherRdd, d7rr)
    val launcherDMU_30 = getXNU_RDD(launcherRdd, d30rr)

    val l_DNU_count_1 = launcherDNU_1.count
    val l_WNU_count_7 = launcherDWU_7.count
    val l_MNU_count_30 = launcherDMU_30.count
    val l_xrrArray = Array(l_DRR_count.toDouble / l_DNU_count_1, l_WRR_count.toDouble / l_WNU_count_7, l_MRR_count.toDouble / l_MNU_count_30)

    val launcherTotal = (for (e <- launcherArray) yield e.toString) ++ (for (e <- l_xrrArray) yield e.toString)
    
    // -----------------微锁屏[新增、活跃用户]计算------------------------------------
    // table :vlock_summary_daily  [DATE,DNU,WNU,MNU,DAU,WAU,MAU,AAU,DRR,WRR,MRR]
    val vlockRdd = sc.textFile(vlockPath).map(getUserDate).persist
    val vlockDNU = getXNU(vlockRdd, date).persist
    val vlockWNU = getXNU(vlockRdd, d7)
    val vlockMNU = getXNU(vlockRdd, d30)
    val vlockDAU = getXAU(vlockRdd, date).persist
    val vlockWAU = getXAU(vlockRdd, d7)
    val vlockMAU = getXAU(vlockRdd, d30).persist
    val vlockDRR = getXRR(vlockRdd, d1rr, date)
    val vlockWRR = getXRR(vlockRdd, d7rr, date)
    val vlockMRR = getXRR(vlockRdd, d30rr, date)

    // (纯桌面用户)：算交集新用户时，要先去除锁屏历史用户
    val l_subtract_DNU = launcherDNU.map(line => line._1).subtract(vlockRdd.map(line => line._1)).count
    // (纯锁屏用户)算交集新用户时，要先去除桌面历史用户
    val v_subtract_DNU = vlockDNU.map(line => line._1).subtract(launcherRdd.map(line => line._1)).count
    vlockRdd.unpersist(false)
    launcherRdd.unpersist(false)
    
    val vlockAccumlateCount = vlockRdd.count
    val v_DNU = vlockDNU.count
    val v_WNU = vlockWNU.count
    val v_MNU = vlockMNU.count
    val v_DAU = vlockDAU.count
    val v_WAU = vlockWAU.count
    val v_MAU = vlockMAU.count
    val v_DRR = vlockDRR.count
    val v_WRR = vlockWRR.count
    val v_MRR = vlockMRR.count
    val vlockArray = Array(date, v_DNU, v_WNU, v_MNU, v_DAU, v_WAU, v_MAU, vlockAccumlateCount)

    // 留存比率记算
    // 留存比率记算
    val vlockDNU_1 = getXNU_RDD(vlockRdd, d1rr)
    val vlockDWU_7 = getXNU_RDD(vlockRdd, d7rr)
    val vlockDMU_30 = getXNU_RDD(vlockRdd, d30rr)

    val v_DNU_count_1 = vlockDNU_1.count
    val v_WNU_count_7 = vlockDWU_7.count
    val v_MNU_count_30 = vlockDMU_30.count
    val v_xrrArray = Array(v_DRR.toDouble / v_DNU_count_1, v_WRR.toDouble / v_WNU_count_7, v_MRR.toDouble / v_MNU_count_30)
    val vlockTotal = (for (e <- vlockArray) yield e.toString) ++ (for (e <- v_xrrArray) yield e.toString)
    
    // -----------------桌面、微锁屏交集[新增、活跃用户]计算------------------------------------
    val interSection_DNU_count = launcherDNU.map(line => line._1).intersection(vlockDNU.map(line => line._1)).count
    launcherDNU.unpersist()
    vlockDNU.unpersist()
    val interSection_DAU = launcherDAU.map(line => line._1).intersection(vlockDAU.map(line => line._1)).count
    launcherDAU.unpersist()
    vlockDAU.unpersist()
    val interSection_MAU = launcherMAU.map(line => line._1).intersection(vlockMAU.map(line => line._1)).count
    launcherMAU.unpersist()
    vlockMAU.unpersist()

    // 日新增桌面、锁屏的总用户
    val moxiu_DNU_count = l_subtract_DNU + v_subtract_DNU + interSection_DNU_count
    val interSection_Array = Array(moxiu_DNU_count.toString, interSection_DAU.toString, interSection_MAU.toString)
    
    //---------------------------------结果打印现示-------------------------------------------
    // table :vlock_summary_daily  [DATE,DNU,WNU,MNU,DAU,WAU,MAU,AAU,DRR,WRR,MRR]
    val launcher_items = Array("DATE", "DNU", "WNU", "MNU", "DAU", "WAU", "MAU", "AAU", "DRR", "WRR", "MRR")
    MoxiuUtil.outPrintDB(print_flg, "mcmp_launcher", "launcher_summary_daily", date, Array(launcherTotal), launcher_items, "=====table: launcher_summary_daily=====")
    val vlock_items = Array("DATE", "DNU", "WNU", "MNU", "DAU", "WAU", "MAU", "AAU", "DRR", "WRR", "MRR")
    MoxiuUtil.outPrintDB(print_flg, "mcmp_vlock", "vlock_summary_daily", date, Array(vlockTotal), vlock_items, "=====table: vlock_summary_daily=====")
    
    val msg = "-------------司南概要页面计算-------------"
    val msg1 = "---table: launcher_vlock_union-----------"
    val launcher_vlock_union_items = Array("DATE", "DNU_LAUNCHER", "DNU_VLOCK", "DNU_OVERLAP", "DNU_TOTAL", "DNU_OVERLAP_OWN_USER_RATIO",
      "DNU_LAUNCHER_OWN_USER_RATIO", "DNU_VLOCK_OWN_USER_RATIO", "DAU_LAUNCHER", "DAU_VLOCK", "DAU_OVERLAP",
      "DAU_TOTAL", "DAU_OVERLAP_OWN_USER_RATIO", "DAU_LAUNCHER_OWN_USER_RATIO", "DAU_VLOCK_OWN_USER_RATIO",
      "MAU_LAUNCHER", "MAU_VLOCK", "MAU_OVERLAP", "MAU_TOTAL", "MAU_OVERLAP_OWN_USER_RATIO", "MAU_LAUNCHER_OWN_USER_RATIO",
      "MAU_VLOCK_OWN_USER_RATIO")

    val dNU_LAUNCHER = l_DNU_count.toString
    val dNU_VLOCK = v_DNU.toString
    // LAUNCHER和VLOCK的交集用户
    val dNU_OVERLAP = interSection_DNU_count.toString
    // 魔秀今日新增用户
    val dNU_TOTAL = moxiu_DNU_count.toString
    // 交集占魔秀用户的比率
    val dNU_OVERLAP_OWN_USER_RATIO = (interSection_DNU_count / moxiu_DNU_count.toDouble).toString
    // 桌面占魔秀用户的比率（纯桌面用户-桌面锁屏交集用户）／今日魔秀用户
    val dNU_LAUNCHER_OWN_USER_RATIO = (l_subtract_DNU / moxiu_DNU_count.toDouble).toString
    // 锁屏占魔秀用户的比率（纯锁屏用户-桌面锁屏交集用户）／今日魔秀用户
    val dNU_VLOCK_OWN_USER_RATIO = (v_subtract_DNU / moxiu_DNU_count.toDouble).toString
    val dAU_LAUNCHER = l_DAU_count.toString
    val dAU_VLOCK = v_DAU.toString
    // 日活交集用户
    val dAU_OVERLAP = interSection_DAU.toString
    // 魔秀用户日活用户
    val dAU_TOTAL = (v_DAU + l_DAU_count - interSection_DAU).toString
    // 日活交集占魔秀用户的比率
    val dAU_OVERLAP_OWN_USER_RATIO = (interSection_DAU / dAU_TOTAL.toDouble).toString
    // 桌面日活占魔秀用户的比率（纯桌面用户-桌面锁屏交集用户）／今日魔秀用户
    val dAU_LAUNCHER_OWN_USER_RATIO = ((l_DAU_count - interSection_DAU) / dAU_TOTAL.toDouble).toString
    // 锁屏日活占魔秀用户的比率（纯锁屏用户-桌面锁屏交集用户）／今日魔秀用户
    val dAU_VLOCK_OWN_USER_RATIO = ((v_DAU - interSection_DAU) / dAU_TOTAL.toDouble).toString
    val mAU_LAUNCHER = l_MAU_count.toString
    val mAU_VLOCK = v_MAU.toString
    // 日活交集
    val mAU_OVERLAP = interSection_MAU.toString
    // 魔秀用户月活用户
    val mAU_TOTAL = (l_MAU_count + v_MAU - interSection_MAU).toString
    // 魔秀交集用户月活用户占魔秀月活用户的比率
    val mAU_OVERLAP_OWN_USER_RATIO = (interSection_MAU / mAU_TOTAL.toDouble).toString
    // 魔秀桌面月活用户占魔秀月活用户的比率
    val mAU_LAUNCHER_OWN_USER_RATIO = ((l_MAU_count - interSection_MAU) / mAU_TOTAL.toDouble).toString
    // 魔秀锁屏月活用户占魔秀月活用户的比率
    val mAU_VLOCK_OWN_USER_RATIO = ((v_MAU - interSection_MAU) / mAU_TOTAL.toDouble).toString

    val summery = Array(date, dNU_LAUNCHER, dNU_VLOCK, dNU_OVERLAP, dNU_TOTAL, dNU_OVERLAP_OWN_USER_RATIO,
      dNU_LAUNCHER_OWN_USER_RATIO, dNU_VLOCK_OWN_USER_RATIO, dAU_LAUNCHER, dAU_VLOCK, dAU_OVERLAP,
      dAU_TOTAL, dAU_OVERLAP_OWN_USER_RATIO, dAU_LAUNCHER_OWN_USER_RATIO, dAU_VLOCK_OWN_USER_RATIO,
      mAU_LAUNCHER, mAU_VLOCK, mAU_OVERLAP, mAU_TOTAL, mAU_OVERLAP_OWN_USER_RATIO, mAU_LAUNCHER_OWN_USER_RATIO, mAU_VLOCK_OWN_USER_RATIO)

    MoxiuUtil.outPrintDB(print_flg, "mcmp_launcher", "launcher_vlock_union", date, Array(summery), launcher_vlock_union_items, msg, msg1)
    sc.stop
  }

  /**
   * 计算用户留存
   * endDate >= 参数日期
   */
  def getXRR(rdd: RDD[(String, (String, String, String))], drr: String, date: String) = rdd.filter(line => ((line._2)._2 == drr && (line._2)._3 == date))

  /**
   * 统计活跃用户
   * endDate >= 参数日期
   */
  def getXAU(rdd: RDD[(String, (String, String, String))], date: String) = rdd.filter(line => (line._2)._3 >= date)

  /**
   * 新增用户统计
   * startDate >= 参数日期
   */
  def getXNU(rdd: RDD[(String, (String, String, String))], date: String) = rdd.filter(line => (line._2)._2 >= date)

  /**
   * 留存比率计算
   */
  def getXNU_RDD(rdd: RDD[(String, (String, String, String))], date: String) = rdd.filter(line => (line._2)._2 == date)
}