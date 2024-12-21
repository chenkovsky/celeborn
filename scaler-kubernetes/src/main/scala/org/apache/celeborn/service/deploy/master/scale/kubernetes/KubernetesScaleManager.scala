package org.apache.celeborn.service.deploy.master.scale.kubernetes

import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager
import org.apache.celeborn.service.deploy.master.scale.IScaleManager

class KubernetesScaleManager extends IScaleManager {

  override def init(statusSystem: AbstractMetaManager): Unit = ???

  override def run(): Unit = ???

  override def stop(): Unit = ???
}
