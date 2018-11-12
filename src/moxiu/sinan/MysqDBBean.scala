package moxiu.sinan

import scala.beans.BeanProperty

class MysqDBBean {
  @BeanProperty var driver = ""
  @BeanProperty var hostAndPort = ""
  @BeanProperty var dataBase = ""
  @BeanProperty var username = ""
  @BeanProperty var password = ""
  def getUrl = "jdbc:mysql://" + hostAndPort + "/" + dataBase + "?useUnicode=true&amp;characterEncoding=UTF-8&amp;autoReconnect=true&amp;failOverReadOnly=false"
}