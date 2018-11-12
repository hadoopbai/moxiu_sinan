package com.moxiu.event

import com.moxiu.db.mysql.ExecutSql


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.alibaba.fastjson.JSON
import scala.collection.mutable.Set
import scala.collection.mutable.Map

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName

import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import java.util.HashMap

import scala.collection.JavaConverters._

/****
 * 版本 1.2
 * 直接解析json 
 */

case class AttributeData(
   muuid : String,
   ctime : String,
   eventid : String,
   calc : String,
   ver : String,
   child : String,
   id : Int,
   att_name : String,
   att_value : String
)

object AttributeDataCalculation {
  
  val separator = "\001"
  
   /***设置日志级别**/
//  Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
 
  def main (args : Array[String]) =  {
    
    if(args.length < 6) {
      System.err.println("<Usage>")
      System.exit(1)  
    }
    
    val Array(sourcePath,database,table,date,targePath,product,_*) = args
    val dict = getDataFromDict(database,0,table)
    val formatDate = date.replaceAll("-", "")
    val subformat = formatDate.substring(2)
    
    println("event_dict size :" + dict.size)
    
    
    /**清空数据库**/
//    truncateDS("mcmp_event_l",0,"event_launcher_ver")
//    truncateDS("mcmp_event_l",0,"event_launcher_child")
        
    val sparkConf = new SparkConf().setAppName("AttributeData Calculation")
    sparkConf.registerKryoClasses(Array(classOf[AttributeData])) // Kryo 序列化
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(sourcePath).persist()
                       
//     /**event_launcher_child ***/         
//       val childList =  data.map(line => (line.split(separator)(26),1)).reduceByKey(_+_).map(f => Array(f._1))
//       val child_item = Array("CHILD")
//      DimensionCore.updateByTypeCollect(childList, "mcmp_event_l", "event_launcher_child", date, child_item,0) 
//     
//      /** event_launcher_ver**/  
//        val  verList =  data.map(line => (line.split(separator)(24),1)).reduceByKey(_+_).map(f => Array(f._1))
//        val ver_item = Array("VER")
//        DimensionCore.updateByTypeCollect(verList,"mcmp_event_l", "event_launcher_ver", date, ver_item,0)     
    
     /**event_product_summary_daily**/

      val content = data.map(line => {
        val json=JSON.parseObject(line)
        val base = json.getJSONObject("base")
        val content = json.getJSONObject("content")
        val imei = base.getString("imei")
        val androidid = base.getString("androidid")
        val muuid = s"${imei}|${androidid}"
        val eventid = content.getString("eventid")
        (eventid,muuid)})
        .filter(f=> dict.contains(f._1))
        .aggregateByKey(ArrayBuffer[String]())((set, value) => set += value, _ ++ _)
        .map(f => {
          val array = f._2.toArray
          (f._1,array.length,array.distinct.length)
        }).persist()
      
      /***跳转汇总表**/
      val summary_day = content.map(f => {
          val content = if(dict.contains(f._1)) dict.get(f._1).get else (null,null,null,null,null)
         ((content._1.toString().toInt % 30 + "|" + content._1.toString() + separator + subformat),"a",JSON.toJSON(new HashMap[String,String](){{put("time",date);put("ch",content._2);put("id",f._1);put("p",f._2 toString);put("u",f._3 toString)}}))
      }).mapPartitions(f => writeToHbase(f,s"mx_event:${product}_summary_daily")).collect() 
      
      /***初始化自定义列表**/
      val init_day = content.map(f => {
          val content = if(dict.contains(f._1)) dict.get(f._1).get else (null,null,null,null,null)
         ((content._1.toString().toInt % 10 + "|" + content._1.toString()),"a",JSON.toJSON(new HashMap[String,String](){{put("time",date);put("ch",content._2);put("id",f._1);put("p",f._2 toString);put("u",f._3 toString)}}))
      }).mapPartitions(f => writeToHbase(f,s"mx_event:${product}_init_daily")).collect()
      
      content.unpersist()
    
    /*** muuid,ctime,ip,ua,eventid,AttributeData1,AttributeData2,AttributeData3,AttributeData4,AttributeData5,AttributeData6,AttributeData7,AttributeData8,AttributeData9,AttributeData10
     ,calc,androidid,ime,model,net,mac,display,ipaddr,install_tamp,ver,timestamp,child,manufacturer,vcode,androidsdk,ic,ts,locale,rominfo 
     * 
     **/

    /**  
     *  输入值: (muuid,ctime,eventid,calc,ver,child,Array(AttributeData1[1-10]))
     *  返回值:  (muuid,ctime,eventid,calc,ver,child,id,att_name,att_value)
     *  
     *   限制属性长度: 属性长度 < 100
     **/
    
     val fetchData = data.map(parseJsonToAttributeData(_,dict)).flatMap(f => f).filter(f => !f.isEmpty).map(f=>f.get).persist()
                        
     data.unpersist()
     
     def param (f : AttributeData, postion : Array[String]) : Array[String] = {    
      for(p <- postion) yield  p match {            
                  case   "muuid" => f.muuid
                  case   "ctime" => f.ctime
                  case   "eventid"  => f.eventid
                  case   "calc" => f.calc
                  case   "ver" => f.ver
                  case   "child" => f.child
                  case   "id" => f.id.toString()
                  case   "att_name" =>f.att_name
                  case   "att_value" =>f.att_value
                  case    _   => ""
               }
     }
     
     /***
      * 功能: 统计 calc值,pv值,uv值
      */
     def calculator(data : RDD[AttributeData], postion : Array[String]) = {     
          data.map(f=> (param(f,postion).mkString(separator),(f.calc,1,f.muuid)))
                                 .aggregateByKey((ArrayBuffer[Double](),ArrayBuffer[Int](),Set[String]()))((set,value) => (set._1 += value._1.toDouble,set._2 += value._2,set._3 += value._3),(a,b)=> (a._1 ++ b._1, a._2 ++ b._2,a._3 ++ b._3))
                                 .map(f => {
                                      val calc = f._2._1.sum
                                      val pv = f._2._2.size
                                      val uv = f._2._3.size
                                      (formatDate,f._1,calc toString(),pv toString(),uv toString(),f._2._3)}).filter(f=>f._4.toInt > 5)
                                  }     

     /**
      * 功能： 求每个单属性的PV和UV，及每个属性下值的PV和UV
      * 输入值(时间,维度,calc值，pv值，uv值,Set[muuid])
      * 
      */
     
      def transformJson(rdd : RDD[(String, String, String, String, String, Set[String])]) = {
       rdd.map(f=>{
           val postion = f._2.lastIndexOf(separator)
           val key = f._2.substring(0,postion)
           val value = f._2.substring(postion + 1)
           (key,(value,f._3,f._4,f._5,f._6))
       }).aggregateByKey((ArrayBuffer[(String,String,String,String)](),ArrayBuffer[Int](),Set[String]()))((set,value) => 
         (set._1 += ((value._1,value._2,value._3,value._4)),set._2 += value._3.toInt,set._3 ++ value._5),(a,b)=> (a._1 ++ b._1, a._2 ++ b._2,a._3 ++ b._3))
        .map(f =>{
           val map = new HashMap[String,java.util.Map[String,String]]()
           val base = new HashMap[String,String](){{put("p", f._2._2.sum.toString());put("u", f._2._3.size.toString())}}
           map.put("mx_totle",base)
           map.put("mx_base",new HashMap[String, String](){{put("time",date)}})        
           f._2._1.map(f => {
             val valueMap = new HashMap[String,String]()
             valueMap.put("c", f._2)
             valueMap.put("p", f._3)
             valueMap.put("u", f._4)
             map.put(f._1, valueMap)                                                                                        
          })         
         (f._1,JSON.toJSON(map))                                                                                    
        })
     }
       
    
    /***
     *  event_launcher_AttributeData_su_daily_cals
     * (muuid,ctime,eventid,calc,ver,child,id,att_name,att_value)
     *  返回字段
     *  (DATE,E_ID,EVENT_ID,EVENT_AttributeData_CH,EVENT_AttributeData,EVENT_VALUE,EVENT_PV.EVENT_UV)
     *  
     * create_namespace 'mx_event_launcher'
     * create 'mx_event_launcher:launche_su_daily','F' 
     */
    
     sc.parallelize(dict.toSeq).map(f => {
       val map = new HashMap[String,Object]()
       val id = f._2._1 toString
       val ch = f._2._2
       val att = f._2._5
       val exist = f._2._3.toString
       map.put("id", id)
       map.put("ch", ch)
       map.put("att", att)
       map.put("exit", exist)
       (product + separator + f._1,"a",JSON.toJSON(map))
     }).mapPartitions(f => writeToHbase(f,"mx_event:product_meta_daily")).collect()
            
      /***汇总表**/
     val AttributeData_su_daily_cals = calculator(fetchData,Array("id","att_name","att_value"))     
     transformJson(AttributeData_su_daily_cals).map(f => 
       (f._1.substring(0,f._1.indexOf(separator)).toInt % 30 +"|"+ f._1.substring(0,f._1.lastIndexOf(separator)) + separator + subformat,f._1.substring(f._1.lastIndexOf(separator)+1),f._2))
         .mapPartitions(f => writeToHbase(f,s"mx_event:${product}_su_daily")).collect()
     
       /***属性_渠道**/  
     val AttributeData_daily_child = calculator(fetchData,Array("id","child","att_name","att_value"))
     transformJson(AttributeData_daily_child).map(f => 
       (f._1.substring(0,f._1.indexOf(separator)).toInt % 30 +"|"+ f._1.substring(0,f._1.lastIndexOf(separator)) + separator + subformat,f._1.substring(f._1.lastIndexOf(separator)+1),f._2))
         .mapPartitions(f => writeToHbase(f,s"mx_event:${product}_child_daily")).collect()
      
        /***属性_版本—_渠道**/
     val AttributeData_daily_child_ver = calculator(fetchData,Array("id","child","ver","att_name","att_value"))
     transformJson(AttributeData_daily_child_ver).map(f => 
       (f._1.substring(0,f._1.indexOf(separator)).toInt % 30 +"|"+ f._1.substring(0,f._1.lastIndexOf(separator)) + separator + subformat,f._1.substring(f._1.lastIndexOf(separator)+1),f._2))
         .mapPartitions(f => writeToHbase(f,s"mx_event:${product}_child_ver_daily")).collect()
         
       /***属性_版本**/  
     val AttributeData_daily_ver = calculator(fetchData,Array("id","ver","att_name","att_value"))
     transformJson(AttributeData_daily_ver).map(f => 
       (f._1.substring(0,f._1.indexOf(separator)).toInt % 30+"|"+ f._1.substring(0,f._1.lastIndexOf(separator)) + separator + subformat,f._1.substring(f._1.lastIndexOf(separator)+1),f._2)
     ).mapPartitions(f => writeToHbase(f,s"mx_event:${product}_ver_daily")).collect() 

     fetchData.unpersist()
     sc.stop()
     
  }
  
  /**
   * product : 完整数据库名字
   * dele_flg :  // 1:为拼接成的数据库名称，其它: 传入数据库名字 
   * (ID,)
   */
  
  def getDataFromDict(product : String, dele_flg : Int, table :String) : collection.mutable.Map[String, (Int,String, Int, Int ,Array[String])] = {
    val sql = s"SELECT ID,EVENT_ID,EVENT_NAME,EVENT_ATTRIBUTE_EXIST,CALC_EXIST,EVENT_ATTRIBUTE FROM ${product}.${table} WHERE EVENT_STATUS = 2"
    val dbInfo = ExecutSql.getDBInfo(product, dele_flg)
    val connection = ExecutSql.getConnection(dbInfo)
    val rs = ExecutSql.selectDate(connection, sql)
    
    val map = scala.collection.mutable.Map[String, (Int,String, Int, Int ,Array[String])]()
    
    /***
     * id : 事件编号
     * eventid :事件ID
     * eventName : 事件名称
     * exist : 是否有参数
     * calc : 计算还是计数
     * AttributeData : 属性列
     */
    
    while(rs.next()) {
      val id        = rs.getInt("ID")
      val eventid   = rs.getString("EVENT_ID")
      val eventName = rs.getString("EVENT_NAME")
      val exist     = rs.getInt("EVENT_ATTRIBUTE_EXIST")
      val calc      = rs.getInt("CALC_EXIST")
      val AttributeData1 = rs.getString("EVENT_ATTRIBUTE")
      map += (eventid -> (id,eventName,exist,calc,if(!(AttributeData1 == null) && AttributeData1.length() != 0) AttributeData1.split(";").toArray else Array(null)))
    }
    connection.close()
    map    
  }  
  
  /**
   * 清空数据
   */
  
  def truncateDS(product : String, dele_flg : Int, table :String) {
    val sql = s"TRUNCATE TABLE ${product}.${table}"
    val dbInfo = ExecutSql.getDBInfo(product, dele_flg)
    val connection = ExecutSql.getConnection(dbInfo)
    ExecutSql.executSql(connection, sql)
    connection.close()    
  }
  
   /**
   * 调用存储过程
   * 存储过程命名方法：数据库名 + partiton_auto
   */
  def callProcess(product : String, dele_flg : Int, date :String) {
    val sql = s"CALL TABLE ${product}_partiton_auto(${date})"
    val dbInfo = ExecutSql.getDBInfo(product, dele_flg)
    val connection = ExecutSql.getConnection(dbInfo)
    ExecutSql.executSql(connection, sql)
    connection.close()    
  }
   
  
  def connectHbase(tablename :String) : Table = {
      val conf = HBaseConfiguration.create()
       conf.set("hbase.zookeeper.property.clientPort","2181");
       conf.set("hbase.zookeeper.quorum", "10.1.0.180,10.1.0.153,10.1.0.150,10.1.0.235,10.1.0.166");
       ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tablename))
  }
  
  /**
   * row:(String,String,Object)
   * 1. rowkey
   * 2. cloumn
   * 3. value
   */
  
  def writeToHbase(row : Iterator[(String, String, Object)], tablename : String) : Iterator[(String, String, Object)]  = {
     val table = connectHbase(tablename)
     val list = new  ArrayList[Put]()
     for(f <- row.toList) {
       val put = new Put(Bytes.toBytes(f._1))
       put.addColumn(Bytes.toBytes("F"),Bytes.toBytes(f._2),Bytes.toBytes(f._3.toString()))
       list.add(put)
     }
     table.put(list)
     table.close()
     row    
  } 
  
  /*** (muuid,ctime,eventid,calc,ver,child,id,att_name,att_value) */ 
  
  def parseJsonToAttributeData(str : String,dict: Map[String, (Int, String, Int, Int, Array[String])]) : Array[Option[AttributeData]] = {
     val json=JSON.parseObject(str)
     val ctime = json.getLong("ctime")
     val base = json.getJSONObject("base")
     val content = json.getJSONObject("content")
     val calc = if(content.getString("calc") == null || content.getString("calc").length() == 0) "0.0" else content.getString("calc")
     val imei = base.getString("imei")
     val androidid = base.getString("androidid")
     val muuid = s"${imei}|${androidid}"
     val child = base.getString("child")
     val ver = base.getString("ver")
     val eventid = content.getString("eventid")
     val mapper = content.getJSONObject("map")
//     if(mapper.isEmpty()) mapper.put("mx_no_name", "mx_no_att") // 没有属性,设置默认属性
     if(dict.contains(eventid)) {
      val att =  dict.get(eventid)
      val id = att.get._1
      for(t <- att.get._5 if(att.get._5.length > 0 && mapper.containsKey(t))) yield Some(AttributeData(muuid,ctime.toString(),eventid,calc.toString(),ver,child,id,t.toString(),mapper.getString(t).toString()))       
     } else Array(None)
  }  
}




