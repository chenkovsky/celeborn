package org.apache.celeborn.service.deploy.master.scale.kubernetes

import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager
import org.apache.celeborn.service.deploy.master.scale.IScaleManager

class KubernetesScaleManager(conf: CelebornConf) extends IScaleManager {

  val client = new KubernetesClientBuilder().build

  override def init(statusSystem: AbstractMetaManager): Unit = ???

  override def run(): Unit = ???

  override def stop(): Unit = ???
}
