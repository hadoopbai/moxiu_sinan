package com.moxiu.calculate.sinan.launcher

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import com.moxiu.shucang.sinan.MoxiuUtil

/**
 * 岳远扬 除桌面、锁屏外产口的计算
 * 桌面、锁屏 的数据在【桌面】产品中已经计算过，司南首画面用到魔秀用户
 */
object UserSummaryCount {
/***
   * productRoot: 留存表路径
   * product: 产品名称
   * date : 日期
   * print_flg : 是否打印结果
   */
  def main(args: Array[String]): Unit = {

    val productRoot = args(0)
    val product = args(1).toString
    val date = args(2).trim
    // 是否打印表数据到文件
    val print_flg = args(3).trim.toBoolean
    // 留存目录
    val productPath = productRoot + "/" + MoxiuUtil.getDate(date, 0, "yyyy/MM/dd")

    val d1rr = MoxiuUtil.getDate(date, -1, "yyyy-MM-dd")
    val d7rr = MoxiuUtil.getDate(date, -7, "yyyy-MM-dd")
    val d30rr = MoxiuUtil.getDate(date, -30, "yyyy-MM-dd")

    val d7 = MoxiuUtil.getDate(date, -6, "yyyy-MM-dd")
    val d30 = MoxiuUtil.getDate(date, -29, "yyyy-MM-dd")

    // -----------------数据表预删除处理------------------------------------
    val product_tableList = Array(product + "_summary_daily")
    MoxiuUtil.deleTable("mcmp_" + product, date, product_tableList, 2)

    val separator = "\001"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName("DRRUserCount")

    def getUserDate(line: String) = {
      val infoList = line.split(separator)
      val user = infoList(0)
      val start = infoList(1).split(" ")(0)
      val end = infoList(2).split(" ")(0)
      (user, (user, start, end))
    }
    // -----------------产品用户[新增、活跃用户]计算------------------------------------
    // table product_summary_daily: [DATE,DNU,WNU,MNU,DAU,WAU,MAU,AAU,DRR,WRR,MRR]
    val productRdd = sc.textFile(productPath).map(getUserDate).persist
    val productDNU = getXNU(productRdd, date).persist
    val productWNU = getXNU(productRdd, d7)
    val productMNU = getXNU(productRdd, d30)
    val productDAU = getXAU(productRdd, date).persist
    val productWAU = getXAU(productRdd, d7)
    val productMAU = getXAU(productRdd, d30).persist
    val productDRR = getXRR(productRdd, d1rr, date)
    val productWRR = getXRR(productRdd, d7rr, date)
    val productMRR = getXRR(productRdd, d30rr, date)

    val productAccumlateCount = productRdd.count
    val l_DNU_count = productDNU.count
    val l_WNU_count = productWNU.count
    val l_MNU_count = productMNU.count
    val l_DAU_count = productDAU.count
    val l_WAU_count = productWAU.count
    val l_MAU_count = productMAU.count
    val l_DRR_count = productDRR.count
    val l_WRR_count = productWRR.count
    val l_MRR_count = productMRR.count
    val productArray = Array(date, l_DNU_count, l_WNU_count, l_MNU_count, l_DAU_count, l_WAU_count, l_MAU_count, productAccumlateCount)

    for (a <- productArray) println(a)

    // 留存比率记算
    val productDNU_1 = getXNU_RDD(productRdd, d1rr)
    val productDWU_7 = getXNU_RDD(productRdd, d7rr)
    val productDMU_30 = getXNU_RDD(productRdd, d30rr)

    val l_DNU_count_1 = productDNU_1.count
    val l_WNU_count_7 = productDWU_7.count
    val l_MNU_count_30 = productDMU_30.count
    val xrrArray = Array(l_DRR_count.toDouble / l_DNU_count_1, l_WRR_count.toDouble / l_WNU_count_7, l_MRR_count.toDouble / l_MNU_count_30)

    val l_xrrArray = for (tmp <- xrrArray) yield if (tmp.isNaN()) 0.0 else tmp
    //    val l_xrrArray = Array(l_DRR_count.toDouble / l_DNU_count_1, l_WRR_count.toDouble / l_WNU_count_7, l_MRR_count.toDouble / l_MNU_count_30)

    val productTotal = (for (e <- productArray) yield e.toString) ++ (for (e <- l_xrrArray) yield e.toString)

    //---------------------------------结果打印现示-------------------------------------------
    // table :product_summary_daily  [DATE,DNU,WNU,MNU,DAU,WAU,MAU,AAU,DRR,WRR,MRR]
    val product_items = Array("DATE", "DNU", "WNU", "MNU", "DAU", "WAU", "MAU", "AAU", "DRR", "WRR", "MRR")
    MoxiuUtil.outPrintDB(print_flg, "mcmp_" + product, product + "_summary_daily", date, Array(productTotal), product_items, "=====table: " + product + "_summary_daily=====")

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