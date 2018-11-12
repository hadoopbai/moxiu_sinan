package moxiu.sinan

import scala.beans.BeanProperty
class BasePropBean extends Serializable {
  @BeanProperty var ipFile = ""
  @BeanProperty var typeId = 0
}