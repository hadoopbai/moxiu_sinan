package com.moxiu.db.mysql

import java.sql.{ DriverManager, Connection, Types, PreparedStatement }
import scala.collection.mutable.{ ArrayBuffer, Map }
import moxiu.sinan.MysqDBBean
import java.sql.SQLException
import java.sql.ResultSet
import org.apache.log4j.Logger

case class SqlInfo(mysqInfo: MysqDBBean, table: String, dataList: Array[Array[String]], ItemList: Array[String])
object ExecutSql {

  Class.forName("com.mysql.jdbc.Driver")
  def getConnection(mysqInfo: MysqDBBean) = DriverManager.getConnection(mysqInfo.getUrl, mysqInfo.getUsername, mysqInfo.getPassword)
  
    /**
   * 获取DB信息
   */
  def getDBInfoByURL(product: String, uptype: Int, url : String, user : String = "admin_mx",passwd : String = "F]D3+XIi50C}qpdD" ) = {
    val mysqDBInfo = new MysqDBBean
//    mysqDBInfo.setHostAndPort("10.1.0.147:3306") /**新司南**/
//     mysqDBInfo.setHostAndPort("10.1.0.21:3306") /**自定义**/
    mysqDBInfo.setHostAndPort(s"${url}:3306") /**老司南**/
    
    // 1:为拼接成的数据库名称，其它：传入数据库名字 
    if (uptype == 1) mysqDBInfo.setDataBase("mcmp_" + product) else mysqDBInfo.setDataBase(product)
    mysqDBInfo.setUsername(user)
    mysqDBInfo.setPassword(passwd)
    mysqDBInfo
  }
  
  /**
   * 获取DB信息
   */
  
  def getDBInfo(product: String, uptype: Int) = {
    val mysqDBInfo = new MysqDBBean
    mysqDBInfo.setHostAndPort("10.1.0.147:3306") /**新司南**/
//     mysqDBInfo.setHostAndPort("10.1.0.21:3306") /**自定义**/
//    mysqDBInfo.setHostAndPort("10.1.0.20:3306") /**老司南**/
    
    // 1:为拼接成的数据库名称，其它：传入数据库名字 
    if (uptype == 1) mysqDBInfo.setDataBase("mcmp_" + product) else mysqDBInfo.setDataBase(product)
    mysqDBInfo.setUsername("admin_mx")
    mysqDBInfo.setPassword("F]D3+XIi50C}qpdD")
    mysqDBInfo
  }

  def instertSqlExec(connection: Connection,sqlInfo: SqlInfo) = {
    val dataList = sqlInfo.dataList
    // 插入的字段名称
    val columnList = sqlInfo.ItemList
    val table = sqlInfo.table
    // 获表字段类型
    val columnTypeMap = getColumnType(connection, table)
    // 更新字段列名类型
    val typeList = (for (column <- columnList) yield (column, columnTypeMap.get(column).get)).map(_._2)
    val sql = "insert into " + sqlInfo.table + columnList.mkString(" (", ",", ")") + " values " + (1 to columnList.size map (f => "?")).mkString("(", ",", ")")
    val pstmt = connection.prepareStatement(sql)
    try {
      // 取消自动提交 
      connection.setAutoCommit(false)
      var flag = 0
      for (dataArray <- dataList) stmtSetParam(typeList.zip(dataArray), pstmt).addBatch()
      pstmt.executeBatch()
      pstmt.close
      connection.commit
    } catch {
      case e: Exception => {
        e.printStackTrace
        connection.rollback
        pstmt.close
      }
    }
  }

  /**
   * 设定SQL参数
   */
  def stmtSetParam(columnInfo: Array[(Int, String)], pstmt: PreparedStatement) = {
    for (i <- 0 until columnInfo.size) {
      val index = i + 1
      val columnType = columnInfo(i)._1
      val value = columnInfo(i)._2     
      if (columnType == Types.BIGINT) {
        if (value.trim != "") pstmt.setLong(index, value.toLong) else pstmt.setNull(index, Types.BIGINT)
      } else {
        columnType match {
          case Types.VARCHAR => if (value.trim != "") pstmt.setString(index, value) else pstmt.setNull(index, Types.VARCHAR)
          case Types.BIGINT  => if (value.trim != "") pstmt.setInt(index, value.toInt) else pstmt.setNull(index, Types.BIGINT)
          case Types.INTEGER => if (value.trim != "") pstmt.setInt(index, value.toInt) else pstmt.setNull(index, Types.INTEGER)
          case Types.DOUBLE  => if (value.trim != "") pstmt.setDouble(index, value.toDouble) else pstmt.setNull(index, Types.DOUBLE)
          case Types.BOOLEAN => if (value.trim != "") pstmt.setBoolean(index, value.toBoolean) else pstmt.setNull(index, Types.BOOLEAN)
          case Types.FLOAT   => if (value.trim != "") pstmt.setFloat(index, value.toFloat) else pstmt.setNull(index, Types.FLOAT)
          case _             =>
        }
      }
    }
    pstmt
  }

  /**
   * 取出列名和类型
   */
  def getColumnType(conn: Connection, tableName: String) = {
    val map = Map[String, Int]()
    val resultSet = conn.getMetaData.getColumns(null, "%", tableName, "%")
    import scala.collection.JavaConverters._
    while (resultSet.next) map += (resultSet.getString("COLUMN_NAME") -> resultSet.getString("DATA_TYPE").toInt)
    map
  }

  /**
   * 删除数据操作
   */
  
  def deleteDate(connection: Connection,dataDate: String, table: String): Unit = {
    // 梯度留存时间
    val sql = if (table.matches(".*_xxrr_grad_daily") ) "delete from " + table + " where TIME_S='" + dataDate + "'"
    else if (table.matches(".*_chn_dxrr_grad_daily") ) "delete from " + table + " where DATE_S='" + dataDate + "'"
    else "delete from " + table + " where date='" + dataDate + "'"
    val statement = connection.createStatement
    statement.executeUpdate(sql)
    statement.close
  }
  
  /**
   * 查询数据
   */
  
  def selectDate(connection: Connection ,sql : String) : ResultSet = {
     val statement = connection.createStatement()
     val resultSet = statement.executeQuery(sql) 
     resultSet
  }
  
   /**
   * 执行数据
   */
   def executSql(connection: Connection ,sql : String)  = {
     val statement = connection.createStatement()
     val resultSet = statement.execute(sql)
  }
}



