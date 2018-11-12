package com.moxiu.mcmp.register

import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.JSON
import java.text.SimpleDateFormat
import scala.collection.mutable.{Set,Map}

import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.moxiu.mcmp.register.Bean.RegisterContent
import com.moxiu.mcmp.register.Bean.RegisterMsg

import com.moxiu.db.mysql.ExecutSql
import java.sql.Connection
import moxiu.sinan.DimensionCore

/**
 * 新增注册用户,累计注册用户计算
 */

object RegisterStatistical {
  
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  
  def main(args : Array[String]) {  
    
    val Array(path,dtime,targe) = args
    
    val produceRederence = getProduceRederence("mcmp_default",0,"product_register_reference","10.1.0.147")
    val produceDatabase = getProduceDatabase("mcmp_default",0,"product_register_database","10.1.0.147")
    
    produceDatabase.mapValues(f => {
       val dbInfo = ExecutSql.getDBInfoByURL(f._1.split("\\.")(0),0,"10.1.0.147")
       val connection = ExecutSql.getConnection(dbInfo)
       deleteByDate(connection,dtime,f._1.toString)
       deleteByDate(connection,dtime,f._2.toString)
    })
  
    val sparkConf = new SparkConf()
                .setAppName("RegisterStatistical")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                
    sparkConf.registerKryoClasses(Array(classOf[RegisterMsg],classOf[RegisterContent]))  // Kryo 序列化
    val sc = new SparkContext(sparkConf)
    
    val rdd = sc.textFile(path).map(JSON.parseObject(_,classOf[RegisterMsg])).filter(f =>(f.getReg() != null && (sdf.format(f.getReg().getTime().toLong * 1000) <= dtime)))
              .map(f => {              
                val userID = f.getId()
                val key = if(f.getReg().getKey() != null) f.getReg().getKey() else "unkown"
                val ip = if(f.getReg().getIp() != null) f.getReg().getIp() else "unkown"
                val time = if(f.getReg().getTime() != null) sdf.format(f.getReg().getTime().toLong * 1000) else "1970-01-01"   
                val payload = if(f.getReg().getPayload() != null) f.getReg().getPayload() else "unkown"
                val appid = if(f.getReg().getAppid() != null && produceRederence.contains(f.getReg().getAppid())) produceRederence.get(f.getReg().getAppid()).get else "launcher"             
                (userID,key,ip,time,appid)  
              }).persist()
              
     /****cumulative registered users total*/
     val cru = rdd.count
     /**new users total **/
     val yestday = rdd.filter(f => f._4 == dtime).count()
   
     val tatolproduct = sc.makeRDD(Seq((dtime,"allproduct",yestday.toInt,cru.toInt)))
    
     /** 分产品统计**/
     rdd.map(f => (f._5,(f._4,f._1)))
       .aggregateByKey(Set[(String,String)]())((x,y) => x += y, (x,y) => x ++ y)
       .map(f => {
           val appid = f._1
           val totalcount = f._2.size
           val newUser = f._2.filter(p=>p._1 == dtime).size
         (dtime,appid,newUser,totalcount)
       }).union(tatolproduct).repartition(1)
         .map(f => {       
           val databases = produceDatabase.get(f._2).get._1
           val dbInfo = ExecutSql.getDBInfoByURL(databases.split("\\.")(0),0,"10.1.0.147")
           val connection = ExecutSql.getConnection(dbInfo)
           InsertByGroup(connection,databases,f._1,f._2,f._3,f._4)
       }).collect()
     
     rdd.unpersist()
     sc.stop()                 
  }
  
  
  /**插入数据库(少量数据)***/
    def InsertByGroup(connection: Connection,table : String, date : String, appid : String, newuser : Int, tatol : Int): Unit = {
      val sql = s"insert into ${table}(DATE,PRODUCT,NRU,ARU) values("+"\"" + date +"\""+","+"\""+ appid + "\""+","+"\""+ newuser+"\""+","+"\""+ tatol +"\");"
      val statement = connection.createStatement
      statement.executeUpdate(sql)
      statement.close
      connection.close()
  }
  
  
  /***数删除据**/
  def deleteByDate(connection: Connection,dataDate: String, table: String): Unit = {
      val sql = "delete from " + table + " where DATE ='" + dataDate + "'"
      val statement = connection.createStatement
      statement.executeUpdate(sql)
      statement.close
      connection.close()
  }
  
  
   /***加载产品对照表**/
   def getProduceRederence(product : String, dele_flg : Int, table : String, url : String) : Map[String,String] =  {
      val produceMap = Map[String,String]() 
      val sql = s"SELECT ID,SERVER,BIGDATA FROM ${product}.${table}"      
      val dbInfo = ExecutSql.getDBInfoByURL(product,dele_flg,url)
      val connection = ExecutSql.getConnection(dbInfo)
      val rs = ExecutSql.selectDate(connection,sql)
      
      while(rs.next()) {
         produceMap += (rs.getString("SERVER") -> rs.getString("BIGDATA"))
      }
      connection.close() 
      produceMap
   }
   
   /***加载产品数据库表对照表**/
   def getProduceDatabase(product : String, dele_flg : Int, table : String, url : String) : Map[String,(String,String)] =  {
      val produceMap = Map[String,(String,String)]() 
      val sql = s"SELECT ID,PRODUCE,DATEBASE_REGCOUNT,DATEBASE_REGMSG FROM ${product}.${table}"      
      val dbInfo = ExecutSql.getDBInfoByURL(product, dele_flg, url)
      val connection = ExecutSql.getConnection(dbInfo)
      val rs = ExecutSql.selectDate(connection,sql)
      
      while(rs.next()) {
         produceMap += (rs.getString("PRODUCE") -> (rs.getString("DATEBASE_REGCOUNT"),rs.getString("DATEBASE_REGMSG")))
      }
      connection.close() 
      produceMap
    }   
}

/**建表语句(样例)**
CREATE TABLE `vlock_user_register_count` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `DATE` varchar(50) CHARACTER SET utf8mb4 DEFAULT NULL,
  `PRODUCE` varchar(20) CHARACTER SET utf8mb4 DEFAULT NULL,
  `DRU` bigint(20) DEFAULT NULL,
  `ARU` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`ID`),
) ENGINE=InnoDB AUTO_INCREMENT=33836 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
*/