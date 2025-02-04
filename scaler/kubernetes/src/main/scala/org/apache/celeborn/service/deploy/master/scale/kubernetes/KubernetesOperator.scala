package org.apache.celeborn.service.deploy.master.scale.kubernetes

import io.fabric8.kubernetes.api.model.PodList

trait KubernetesOperator {
  def workerPodList(): PodList
  def scaleWorkerStatefulSetReplicas(replicas: Int): Unit
  def workerName(idx: Int): String
}
