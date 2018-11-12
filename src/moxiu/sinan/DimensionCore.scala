package moxiu.sinan

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.moxiu.shucang.sinan.MoxiuUtil

object DimensionCore {

  val separator = "\001"
  /**
   * 适用于渠道分析、留存趋势分析、新增用户、活跃用户计算
   * 按多维度计算当日、次日、7日、30日【DAU、WAU、 MAU、DNU、WNU、MNU、DRR、WRR、MRR】
   */
  def dimensionCalculate(sc: SparkContext,
                         dimensionRootPath: String,
                         dimensionPath: String,
                         liucunRDD: RDD[Array[String]],
                         dataDate: String,
                         dimensionIndexs: Array[Int],
                         liuCunIndexs: Array[Int]) = {
    MoxiuUtil.HDFSInit

    // 获取文件列表。按维度分布计算[日期、维度...、DAU、WAU、 MAU、DNU、WNU、MNU]
    val (wauList, mauList, drrDateList) = getDimensionFileList(dimensionRootPath, dataDate, new Path(dimensionRootPath))
    //  根据维度计算当天【DAU、WAU、 MAU、DNU、WNU、MNU】
    val xANUResult = xANU_Calculate(sc, dimensionPath, wauList, mauList, liucunRDD, dataDate, dimensionIndexs, liuCunIndexs, -6, -29)

    /**
     * aggregateByKey 计算
     * 把想同维度的key聚集在一起，然后取出DRR,WRR,MRR的比率
     */

    def setDateList(drrDateList: Array[String])(line: Tuple2[String, Map[String, String]]) = {
      // line: Tuple2[key, Iterable[(date, 留存率)]]
      val keys = line._1
      val map = line._2
      //  xrr 留存率
      val xddArray = for (date <- drrDateList) yield map.getOrElse(date, "0")
      (keys, xddArray)
    }
    // 按维度统计用户 DRR,WRR,MRR量
    val xRRList = xRR_Calculate(sc, dimensionPath, liucunRDD, drrDateList, dimensionIndexs, liuCunIndexs).reduceLeft((a, b) => a.union(b))
      .aggregateByKey(Map[String, String]())((a, b) => a += b, _ ++ _).map(setDateList(drrDateList))
    xANUResult.leftOuterJoin(xRRList).map(line => {
      val keys = line._1.split(separator)
      val xANU_info = line._2._1
      // DRR,WRR,MRR 计算
      val xrrArray = if (line._2._2 == None) Array[String]("0", "0", "0") else line._2._2.get
      val date = xANU_info._1
      // 日活、新增
      val Array(manu, wanu, danu) = xANU_info._2
      val array = ArrayBuffer[String]()
      array += date
      for (value <- line._1.split(separator)) array += value
      array += danu._1
      array += wanu._1
      array += manu._1
      array += danu._2
      array += wanu._2
      array += manu._2
      for (value <- xrrArray) array += value
      // 日期、维度...、DAU、WAU、 MAU、DNU、WNU、MNU、DRR、WRR、MRR
      array.toArray
    })
  }

  /**
   * 编历维度文件列表
   * 返回date日期下计算--[活跃、新增用DAU,WAU,MAU]的维度文件列表,以及计算留存用的日期
   */
  def getDimensionFileList(dimensionRootPath: String, date: String, hdfsDimensionPath: Path) = {
    // 编历维度表文件夹用--[活跃用户计算用DAU,WAU,MAU]
    val dataDate_format = MoxiuUtil.getDate(date, 0, "yyyy/MM/dd")
    val wauFileList = MoxiuUtil.selectDateFolder(hdfsDimensionPath, MoxiuUtil.getDate(date, -6, "yyyy/MM/dd"), dataDate_format)
    val mauFileList = MoxiuUtil.selectDateFolder(hdfsDimensionPath, MoxiuUtil.getDate(date, -29, "yyyy/MM/dd"), dataDate_format)

    //[留存用户计算用DRR,WRR,MRR]
    val drr_1 = MoxiuUtil.getDate(date, -1, "yyyy-MM-dd")
    val drr_7 = MoxiuUtil.getDate(date, -7, "yyyy-MM-dd")
    val drr_30 = MoxiuUtil.getDate(date, -30, "yyyy-MM-dd")
    val drrDateList = Array(drr_1, drr_7, drr_30)
    Console println "---------wau path List------------"
    wauFileList foreach println
    Console println "---------Mau path List------------"
    mauFileList foreach println
    (wauFileList, mauFileList, drrDateList)
  }

  /**
   * 按版本,渠道分布计算[日期、渠道、版本、DAU、WAU、 MAU、DNU、WNU、MNU]
   * dateDir:用以统计当天日维度表
   * wauList：用以统计维度表周所有数据
   * mauList：用以统计维度表月所有数据
   * liucunRDD:留存表RDD
   * dimensionIndex:维度index
   * liuchunIndex:维度index
   */
  def xANU_Calculate(sc: SparkContext,
                     date_dimensionPath: String,
                     wauList: List[String],
                     mauList: List[String],
                     liucunRDD: RDD[Array[String]],
                     dataDate: String,
                     //  日维度表，维度index
                     dimensionIndex: Array[Int],
                     // 留存表，维度index
                     liuchunIndex: Array[Int],
                     days_1: Int,
                     days_2: Int) = {
    val date_6 = MoxiuUtil.getDate(dataDate, days_1, "yyyy-MM-dd")
    val date_29 = MoxiuUtil.getDate(dataDate, days_2, "yyyy-MM-dd")

    // 按维度统计日活用户量 DAU,WAU,MAU
    val dimensions_DAU = mx_AU(getRDD(sc, date_dimensionPath), dimensionIndex).persist
    val dimensions_WAU = mx_AU(getRDD(sc, wauList.mkString(",")), dimensionIndex).persist
    val dimensions_MAU = mx_AU(getRDD(sc, mauList.mkString(",")), dimensionIndex).persist

    // 按维度统计用户 DNU,WNU,MNU量
    val dimensions_DNU = mx_xNU(liucunRDD, dataDate, dataDate, liuchunIndex).persist
    val dimensions_WNU = mx_xNU(liucunRDD, date_6, dataDate, liuchunIndex).persist
    val dimensions_MNU = mx_xNU(liucunRDD, date_29, dataDate, liuchunIndex).persist

    // 区间时间内的新增用户一定包涵在活跃用户里
    val d_AU_join_NU = mx_AuJoinNu(dimensions_DAU, dimensions_DNU).persist
    val w_AU_join_NU = mx_AuJoinNu(dimensions_WAU, dimensions_WNU).persist
    val m_AU_join_NU = mx_AuJoinNu(dimensions_MAU, dimensions_MNU).persist

    // 释放内存数据集
    dimensions_DAU.unpersist()
    dimensions_WAU.unpersist()
    dimensions_MAU.unpersist()
    dimensions_DNU.unpersist()
    dimensions_WNU.unpersist()
    dimensions_MNU.unpersist()

    // 月活跃用户群一定包涵月新增、周活跃、周新增、日活跃、日新增用户
    val au_nuRSList = m_AU_join_NU.leftOuterJoin(w_AU_join_NU).leftOuterJoin(d_AU_join_NU).map { f =>
      {
        val keys = f._1
        // (((月活跃、月新增), (周活跃、周新增))，(日活跃、日新增))
        // (((String, String), Option[(String, String)]), Option[(String, String)])
        val m_w_d_data = f._2
        // ((月活跃、月新增), (周活跃、周新增))
        val mAuJoninmNu = m_w_d_data._1
        // (月活跃、月新增)
        val manu = mAuJoninmNu._1
        // (周活跃、周新增)
        val wanu = if (mAuJoninmNu._2 == None) ("0", "0") else mAuJoninmNu._2.get
        // (日活跃、日新增)
        val danu = if (m_w_d_data._2 == None) ("0", "0") else m_w_d_data._2.get
        (keys, (dataDate, Array(manu, wanu, danu)))
      }
    }
    d_AU_join_NU.unpersist()
    w_AU_join_NU.unpersist()
    m_AU_join_NU.unpersist()
    au_nuRSList
  }

  /**
   *  过滤dataArray中指定的元素不匹配"^[0-9|a-z|A-Z].*"的数据
   */
  def InvalidFilter(dataArray: Array[Array[String]], indexList: Array[Int]) = {
    val array = ArrayBuffer[Array[String]]()
    for (e <- dataArray) if ((indexList.map(index => if (e(index).matches("^[0-9|a-z|A-Z].*")) 0 else 1)).sum < 1) array += e
    array.toArray
  }

  /**
   * DAU DNU关联
   * 过滤掉 日活量为1的记录
   * 有一些解析有误的数据
   * insert into launcher_device_channel_daily (DATE,CHANNEL,MODEL,BRAND,DPI,DNU,DAU) values ("2017-08-25","A-360","PocketBook SURFpad 3 (10,1")","Obreey","800*1232",0,1)
   */
  def dauJoinDnu(dataDate: String, DAU: RDD[(String, String)], DNU: RDD[(String, String)]) = DAU.leftOuterJoin(DNU).map(dAuJoinNu(dataDate))
  def updateCollect(rdd: RDD[Array[String]], product: String, subTable: String, date: String, itemsList: Array[String]) = rdd.mapPartitions(MoxiuUtil.rDDUpdateDB(product, subTable, date, itemsList), true).collect
  def updateByTypeCollect(rdd: RDD[Array[String]], product: String, subTable: String, date: String, itemsList: Array[String],uptype: Int) = rdd.mapPartitions(MoxiuUtil.rDDUpdateDBByType(product, subTable, date, itemsList,uptype), true).saveAsTextFile("_temporary/"+java.util.UUID.randomUUID().toString())
  
  /**
   * DAU DNU拼接
   */
  
  def dAuJoinNu(date: String)(line: Tuple2[String, Tuple2[String, Option[String]]]) = {
    val d_au_nu = line._2
    val dau = d_au_nu._1
    val dnu = if (d_au_nu._2 == None) "0" else d_au_nu._2.get
    val array = ArrayBuffer[String]()
    array += date
    for (value <- line._1.split(separator)) array += value
    // 数量
    array += dnu
    array += dau
    array.toArray
  }

  /**
   * DAU DNU DRR 拼接
   */
  def dAuJoinNuJoinDRR(date: String)(line: (String, ((String, Option[String]), Option[(String, String)]))) = {
    val dAU_DNU = line._2._1
    val dau = dAU_DNU._1
    val dnu = if (dAU_DNU._2 == None) "0" else dAU_DNU._2.get
    val drr = if (line._2._2 == None) "0" else (line._2._2.get)._2
    val array = ArrayBuffer[String]()
    array += date
    // 过滤掉乱码或取值串行的数据
    for (value <- line._1.split(separator)) array += value
    // 数量
    array += dnu
    array += dau
    array += drr
    array.toArray
  }

  // HDFS文件转为RDD
  def getRDD(sc: SparkContext, dimensionPath: String) = sc.textFile(dimensionPath).map(_.split(separator))
  // 纬度拼接
  def dimConcat(infoList: Array[String], dimensionIndex: Array[Int]) = (for (index <- dimensionIndex) yield infoList(index)).mkString(separator)
  /**
   * xAU.leftOuterJoin(xNU)关联成一个数据集，没有的字段补"0"
   */
  def mx_AuJoinNu(auRdd: RDD[(String, String)], nuRdd: RDD[(String, String)]) = {
    auRdd.leftOuterJoin(nuRdd).map(xau_xnu => {
      val keys = xau_xnu._1
      val xAU_NU = xau_xnu._2
      val xNU = if (xAU_NU._2 == None) "0" else xAU_NU._2.get
      // key ,(活跃量,新增量)
      (keys, (xAU_NU._1, xNU))
    })
  }

  /**
   * check 欲统计纬度是否为空，空：false,非空：true
   * dimensionIndex：指定维度在参数array中的index值
   */
  def dimensionFilter(array: Array[String], dimensionIndex: Array[Int]) = {
    val vaulesList = for (index <- dimensionIndex) yield { if (array(index).trim != "") 0 else 1 }
    if (vaulesList.sum > 0) false else true
  }

  /**
   * 按指定的维度统计活跃用户量。
   * dimensionIndex：指定维度的index值
   */
  def mx_AU(rdd: RDD[Array[String]], dimensionIndex: Array[Int]) = {
    // 日纬度表： 0:userId,1:ctime,2:版本,3:渠道，4:品牌，5:机型，6:网络，7:分辨率，8:安卓版本，9:省，10:市，11:运营商
    // 去除相同用户
    rdd.filter(array => dimensionFilter(array, dimensionIndex)).map(infoList => ((dimConcat(infoList, dimensionIndex), infoList(0))))
      .aggregateByKey(Set[String]())((set, value) => set + value, _ ++ _).map(valuse => (valuse._1, valuse._2.size.toString))
  }

  /**
   * 按指定的维度统计新增用户量。
   * dimensionIndex：指定维度的index值
   * startDate：开始统计日期yyyy-MM-dd （闭区间）
   * endDate：终止统计日期yyyy-MM-dd
   */
  def mx_xNU(rdd: RDD[Array[String]], startDate: String, endDate: String, dimensionIndex: Array[Int]) = {
    // 留存表：userId,startDate,lastDate,版本,渠道，品牌，机型，网络，分辨率，安卓版本，省，市,运营商
    rdd.filter(array => dimensionFilter(array, dimensionIndex) && array(1) >= startDate && array(1) <= endDate)
      .map(array => (dimConcat(array, dimensionIndex), 1)).reduceByKey(_ + _).map(line => (line._1, line._2.toString))
  }

  /**
   * 从留存表中抽出按装时间是要计算RR的用户范围
   * (比如计算范围为：DRR,WRR,MRR,那么用户时间范围为DRR〜MRR，让数据量变少（初步过滤）,具体单个计算时)
   */
  def liuCunFilter(liucunRDD: RDD[Array[String]], drrDateList: Array[String], dimensionIndex: Array[Int]) = 
    liucunRDD.filter(array => dimensionFilter(array, dimensionIndex) && drrDateList.contains(array(1).trim))

  //----------按版本,渠道维度统计用户 DRR,WRR,MRR量-------------------------------
  /**
   * 从日维度表中抽出以联合维度为key的用户，最新的版本,渠道信息
   */
  def mx_xRR_Weidu(sc: SparkContext, dimensionPath: String, dimensionIndex: Array[Int]) = {
    // userId，ctime,版本,渠道，品牌，机型，网络，分辨率，安卓版本，省，市，运营商
    getRDD(sc, dimensionPath).filter(array => dimensionFilter(array, dimensionIndex)).map {
      // userId,维度+separator+维度
      infoList => (infoList(0), (infoList(0) + separator + (for (index <- dimensionIndex) yield infoList(index)).mkString(separator), infoList(1)))
      // userId,维度+separator+维度
    }.aggregateByKey(List[(String, String)]())((a, b) => b :: a, _ ::: _).map(valuse => valuse._2.sortBy(_._2).reverse(0)._1)
  }

  /**
   * 按版本,渠道维度统计用户 DRR,WRR,MRR量.
   * 留存表和维度表取交集.
   */
  def xRR_Core(liuCunRdd: RDD[Array[String]], weiduRdd: RDD[String], rrDate: String, liuchunIndex: Array[Int]) = {
    //liuCunRdd 里面只有计算DRR，WRR，MRR日期的数据
    val lc_dimensionRDD = liuCunRdd.filter(_(1) == rrDate).map(array => array(0) + separator + (for (index <- liuchunIndex) yield array(index)).mkString(separator)).persist
    //userId,版本,渠道 ->去掉userId
    val intersectionRDD = lc_dimensionRDD.intersection(weiduRdd).map(line => (line.split(separator).tail.mkString(separator), 1)).reduceByKey(_ + _)
    val newUserRDD = lc_dimensionRDD.map(line => (line.split(separator).tail.mkString(separator), 1)).reduceByKey(_ + _)
    lc_dimensionRDD.unpersist()
    intersectionRDD.leftOuterJoin(newUserRDD).map { user =>
      {
        val key = user._1
        val values = user._2
        val xrrCount = values._1
        val newCount = if (values._2 == None) 0 else values._2.get
        val rate = if (newCount == 0) "0" else (xrrCount / newCount.toDouble).toString
        (key, (rrDate, rate))
      }
    }
  }

  /**
   * 按【版本,渠道】留存计算
   * drrDateList: [drrDate,wrrDate,mrrDate]
   * array与drrDateList一一对应
   */
  def xRR_Calculate(sc: SparkContext,
                    dimensionPath: String,
                    liuCunRdd: RDD[Array[String]],
                    drrDateList: Array[String],
                    dimensionIndex: Array[Int],
                    liuchunIndex: Array[Int]) = {
    // 留存表 按时间过滤，纬度不为空
    val lc_rdd = liuCunFilter(liuCunRdd, drrDateList, liuchunIndex)
    // 当时的维度统计计算
    val weiduRDD = mx_xRR_Weidu(sc, dimensionPath, dimensionIndex)
    for (drrDate <- drrDateList) yield xRR_Core(lc_rdd, weiduRDD, drrDate, liuchunIndex)
  }
}