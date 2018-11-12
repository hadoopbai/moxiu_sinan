package com.moxiu.shucang.sinan

import java.util.Calendar
import scala.collection.mutable.Set
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import moxiu.sinan.MysqDBBean
import com.moxiu.db.mysql.SqlInfo
import com.moxiu.db.mysql.ExecutSql

object MoxiuUtil {

  /**
   * 获取日期
   * dateStr 格式：yyyy-MM-dd
   * 返回日期格式：returnFormat
   */
  def getDate(dateStr: String, dayNum: Int, returnFormat: String) = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse(dateStr)
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_MONTH, dayNum)
    val dateFormat_retrun = new SimpleDateFormat(returnFormat)
    dateFormat_retrun.format(calendar.getTime)
  }

  //-----------------------HDFSFileSytem 部分------------------------------------
  var HDFSFileSytem: FileSystem = null
  // 获取hdfs
  def HDFSInit = (HDFSFileSytem = FileSystem.get(new Configuration))

  //  判断此文件或目录在ＨＤＦＳ上是否存在
  def isFileExists(path: Path) = HDFSFileSytem.isDirectory(path)

  // 遍历正个rootPath的每一个path，选出文件夹path放入pathSet
  def getPathList(rootPath: Path) = {
    val pathSet = Set[Path]()
    def pathList(rootPath: Path) {
      if (isFileExists(rootPath) && HDFSFileSytem.isDirectory(rootPath)) {
        val listStatus = HDFSFileSytem.listStatus(rootPath).map { fileInfo => fileInfo.getPath }
        listStatus.foreach(subpath => if (HDFSFileSytem.isDirectory(subpath)) { pathSet += subpath; pathList(subpath) })
      }
    }
    pathList(rootPath)
    pathSet
  }

  /**
   * 举例：
   * rootPath: "/user/jp-spark/liucun_process/launcher/data/dayExtract")
   * StartData: "2017/07/19"
   * endDate:"2017/07/25"
   * 返回：/user/jp-spark/liucun_process/launcher/data/dayExtract／【2017/07/19〜2017/07/25】7天的路径,闭全区间
   */
  def selectDateFolder(rootPath: Path, StartData: String, endDate: String = "9999/99/99") = {
    val pattern = ".*/[0-9]{4}/[0-9]{2}/[0-9]{2}$".r
    val matcher = "[0-9]{4}/[0-9]{2}/[0-9]{2}$".r
    getPathList(rootPath).filter(p => {
      val path = p.toString
      // 匹配年／月／日的路径
      val match_flg = pattern.pattern.matcher(path).matches
      if (match_flg) {
        // 获取年月日
        val dateStr = matcher.findFirstMatchIn(path).get.toString
        if (dateStr >= StartData && dateStr <= endDate) true else false
      } else false
    }).map(_.toString).toList
  }

  /**
   * RDD mapPartitions 更新数据库
   */
  def rDDUpdateDB(product: String, subTable: String, date: String, itemsList: Array[String])(it: Iterator[Array[String]]) = {
    val data = it.toArray
    val table = product + subTable
    executMysql(date, product, table, data, itemsList, 1)
    data.toIterator
  }
  
    /**
   * RDD mapPartitions 更新数据库(指定数据库)
   */
  def rDDUpdateDBByType(product: String, subTable: String, date: String, itemsList: Array[String],uptype: Int)(it: Iterator[Array[String]]) = {
    val data = it.toArray
    val table = subTable
    executMysql(date, product, table, data, itemsList, uptype)
    data.toIterator
  }
  /**
   *  结果打印现示
   */
  def updatePrint(print_flg: Boolean, product: String, subTable: String, date: String, data: Array[Array[String]], itemsList: Array[String], msgs: String*) = {
    if (print_flg) {
      for (msg <- msgs)
        Console println msg
      Console println itemsList.mkString("[", ",", "]")
    }
    executMysql(date, product, product + subTable, data, itemsList, 1)
    // 打印表内容
    if (print_flg) data.foreach(a => println(a.mkString("\t")))
  }
  /**
   * driver 打印
   */
  def outPrint(print_flg: Boolean, data: Array[Array[String]], itemsList: Array[String], msgs: String*) = {
    if (print_flg) {
      for (msg <- msgs)
        Console println msg
      Console println itemsList.mkString("[", ",", "]")
      // 打印表内容
      if (print_flg) data.foreach(a => println(a.mkString("\t")))
    }
  }

  /**
   *  结果打印现示
   */
  def outPrintDB(print_flg: Boolean, product: String, table: String, date: String, data: Array[Array[String]], itemsList: Array[String], msgs: String*) = {
    if (print_flg) {
      for (msg <- msgs)
        Console println msg
        Console println itemsList.mkString("[", ",", "]")
    }
    executMysql(date, product, table, data, itemsList, 2)
    // 打印表内容
    if (print_flg) data.foreach(a => println(a.mkString("\t")))
  }

  /**
   * mysql
   * uptype: 1:为拼接成的数据库名称，其它：传入数据库名字
   */
  def executMysql(dataDate: String, product: String, table: String, dataList: Array[Array[String]], ItemList: Array[String], uptype: Int) = {
    val dbInfo = ExecutSql.getDBInfo(product, uptype)
    val sqlInfo = SqlInfo(dbInfo, table, dataList, ItemList)
    val connection = ExecutSql.getConnection(dbInfo)
    try {
      ExecutSql.instertSqlExec(connection, sqlInfo)
      connection.close
    } catch {
      case e: Exception => {
        e.printStackTrace
        connection.close
      }
    }
  }
  
  /**
   * 删除数据表
   * dele_flg: 1:为拼接成的数据库名称，其它：传入数据库名字 
   */
  def deleTable(product: String, dataDate:String,tbList: Array[String],dele_flg:Int) = {
    val dbInfo = ExecutSql.getDBInfo(product, dele_flg)
    val connection = ExecutSql.getConnection(dbInfo)
    tbList.foreach(table => {
     val start = System.currentTimeMillis()/1000
      Console println("table deleting ...... "+  dbInfo.getDataBase+ "." + product + table)
      ExecutSql.deleteDate(connection, dataDate, if (dele_flg == 1) product+table else table)
      Console println("table deleted taking "+ (System.currentTimeMillis()/1000- start) +"s")
    })
    connection.close
  }
}