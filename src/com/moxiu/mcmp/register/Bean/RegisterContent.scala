package com.moxiu.mcmp.register.Bean

import scala.beans.BeanProperty

class RegisterContent extends Serializable {
  
   @BeanProperty var key = ""
   @BeanProperty var ip = ""
   @BeanProperty var time = ""
   @BeanProperty var payload = ""
   @BeanProperty var appid = ""
  
}