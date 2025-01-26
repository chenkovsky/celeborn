package org.apache.celeborn.service.deploy.master.scale.kubernetes

import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.api.model.apps.StatefulSet
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import org.apache.celeborn.common.exception.CelebornException
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

object KubernetesOperator {
  val ENV_POD_NAME = "POD_NAME"
  val ENV_POD_NAMESPACE = "POD_NAMESPACE"
  val LABEL_APP_NAME = "app.kubernetes.io/name"
  val LABEL_ROLE = "app.kubernetes.io/role"
  val ROLE_WORKER = "worker"
  val POD_PHASE_PENDING = "Pending"
}


class KubernetesOperator {

  import KubernetesOperator._

  protected val client: KubernetesClient = new KubernetesClientBuilder().build

  protected val podNamespace: String = {
    System.getenv(ENV_POD_NAMESPACE)
  }

  if (StringUtils.isEmpty(podNamespace)) {
    throw new CelebornException("environment POD_NAMESPACE is empty")
  }

  protected val podName: String = System.getenv(ENV_POD_NAME)
  if (StringUtils.isEmpty(podName)) {
    throw new CelebornException("environment POD_NAME is empty")
  }

  protected val currentPod: StatefulSet = client.apps().statefulSets().inNamespace(podNamespace).withName(podName).get()
  protected val currentInstance: String = currentPod.getMetadata.getLabels.get(LABEL_APP_NAME)

  if (StringUtils.isEmpty(currentInstance)) {
    throw new CelebornException(s"label ${LABEL_APP_NAME} is empty")
  }

  protected val workerStatefulSetName: String = {
    val statefulSet = client.apps().statefulSets().withLabel(
      LABEL_APP_NAME, currentInstance
    ).withLabel(
      LABEL_ROLE, ROLE_WORKER
    ).list().getItems
    if (statefulSet.size() != 1) {
      throw new CelebornException("worker statefulSet is not unique")
    }
    statefulSet.get(0).getMetadata.getName
  }

  def workerIndex(name: String): Int = {
    name.substring(workerStatefulSetName.length + 1).toInt
  }

  def workerName(idx: Int): String = {
    s"${workerStatefulSetName}-${idx}"
  }

  def workerPodList(): PodList = {
    client.pods().withLabel(
      LABEL_APP_NAME, currentInstance
    ).withLabel(
      LABEL_ROLE, ROLE_WORKER
    ).list()
  }

  def hasPending(podList: PodList): Boolean = podList.getItems.asScala.exists(_.getStatus.getPhase == POD_PHASE_PENDING)

  def workerStatefulSet(): StatefulSet = {
    client.apps().statefulSets().withName(workerStatefulSetName).get()
  }

  def scaleWorkerStatefulSetReplicas(replicas: Int): Unit = {
    client.apps().statefulSets().withName(workerStatefulSetName).scale(replicas)
  }

}
