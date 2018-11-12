package com.moxiu.mcmp.register.Bean

import scala.beans.BeanProperty

class RegisterMsg extends Serializable {
  
  @BeanProperty var id = ""
  @BeanProperty var reg : RegisterContent = null
  
}