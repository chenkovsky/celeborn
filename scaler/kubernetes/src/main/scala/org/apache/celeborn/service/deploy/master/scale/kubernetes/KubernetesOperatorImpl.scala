/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.master.scale.kubernetes

import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.api.model.apps.StatefulSet
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import org.apache.celeborn.common.exception.CelebornException
import org.apache.commons.lang3.StringUtils

object KubernetesOperatorImpl {
  /** Environment variable for pod name */
  val ENV_POD_NAME = "POD_NAME"
  /** Environment variable for pod namespace */
  val ENV_POD_NAMESPACE = "POD_NAMESPACE"
  /** Label key for application name */
  val LABEL_APP_NAME = "app.kubernetes.io/name"
  /** Label key for role */
  val LABEL_ROLE = "app.kubernetes.io/role"
  /** Value for worker role label */
  val ROLE_WORKER = "worker"
  /** Pod phase constant for pending status */
  val POD_PHASE_PENDING = "Pending"
}

/**
 * Implementation of KubernetesOperator that provides functionality to manage Celeborn workers
 * in a Kubernetes environment. This class handles interactions with the Kubernetes API to
 * manage worker pods and StatefulSets.
 */
class KubernetesOperatorImpl extends KubernetesOperator {

  import KubernetesOperatorImpl._

  /** Kubernetes client for API interactions */
  protected val client: KubernetesClient = new KubernetesClientBuilder().build

  /** Namespace where the pods are running */
  protected val podNamespace: String = {
    System.getenv(ENV_POD_NAMESPACE)
  }

  // Validate pod namespace
  if (StringUtils.isEmpty(podNamespace)) {
    throw new CelebornException("environment POD_NAMESPACE is empty")
  }

  /** Name of the current pod */
  protected val podName: String = System.getenv(ENV_POD_NAME)
  if (StringUtils.isEmpty(podName)) {
    throw new CelebornException("environment POD_NAME is empty")
  }

  /** StatefulSet of the current pod */
  protected val currentPod: StatefulSet = client.apps().statefulSets().inNamespace(podNamespace).withName(podName).get()
  /** Instance name from the pod labels */
  protected val currentInstance: String = currentPod.getMetadata.getLabels.get(LABEL_APP_NAME)

  if (StringUtils.isEmpty(currentInstance)) {
    throw new CelebornException(s"label ${LABEL_APP_NAME} is empty")
  }

  /**
   * Name of the worker StatefulSet, determined by finding the unique StatefulSet
   * with matching app name and worker role labels
   */
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

  /**
   * Generates the name for a worker pod at the given index.
   * The name follows the Kubernetes StatefulSet naming convention: <statefulset-name>-<index>
   */
  def workerName(idx: Int): String = {
    s"${workerStatefulSetName}-${idx}"
  }

  /**
   * Retrieves the list of all worker pods by filtering pods with matching app name
   * and worker role labels
   */
  def workerPodList(): PodList = {
    client.pods().withLabel(
      LABEL_APP_NAME, currentInstance
    ).withLabel(
      LABEL_ROLE, ROLE_WORKER
    ).list()
  }

  /**
   * Scales the worker StatefulSet to the specified number of replicas
   * by updating the StatefulSet's replica count
   */
  def scaleWorkerStatefulSetReplicas(replicas: Int): Unit = {
    client.apps().statefulSets().withName(workerStatefulSetName).scale(replicas)
  }
}
