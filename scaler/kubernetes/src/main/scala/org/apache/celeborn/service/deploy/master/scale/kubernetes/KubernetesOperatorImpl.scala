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
import org.apache.commons.lang3.StringUtils

import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.internal.Logging

object KubernetesOperatorImpl {

  /** Environment variable for pod name */
  val ENV_WORKER_STATEFUL_SET_NAME = "WORKER_STATEFUL_SET_NAME"

  /** Environment variable for pod namespace */
  val ENV_POD_NAMESPACE = "POD_NAMESPACE"

  /** Pod phase constant for pending status */
  val POD_PHASE_PENDING = "Pending"
}

/**
 * Implementation of KubernetesOperator that provides functionality to manage Celeborn workers
 * in a Kubernetes environment. This class handles interactions with the Kubernetes API to
 * manage worker pods and StatefulSets.
 */
class KubernetesOperatorImpl extends KubernetesOperator with Logging {

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

  /**
   * Name of the worker StatefulSet, determined by finding the unique StatefulSet
   * with matching app name and worker role labels
   */
  protected val workerStatefulSetName: String = {
    System.getenv(ENV_WORKER_STATEFUL_SET_NAME)
  }

  // Validate worker stateful set name
  if (StringUtils.isEmpty(workerStatefulSetName)) {
    throw new CelebornException("environment WORKER_STATEFUL_SET_NAME is empty")
  }

  /**
   * Generates the name for a worker pod at the given index.
   * The name follows the Kubernetes StatefulSet naming convention: <statefulset-name>-<index>
   */
  def workerName(idx: Int): String = {
    s"${workerStatefulSetName}-${idx}"
  }

  def workerStatefulSet(): StatefulSet = {
    val statefulSet =
      client.apps().statefulSets().inNamespace(podNamespace).withName(workerStatefulSetName).get()
    if (statefulSet == null)
      throw new CelebornException("StatefulSet not found: " + workerStatefulSetName)
    statefulSet
  }

  /**
   * Retrieves the list of all worker pods by filtering pods with matching app name
   * and worker role labels
   */
  def workerPodList(): PodList = {
    val statefulSet = workerStatefulSet()
    // 获取标签选择器
    val selector = statefulSet.getSpec.getSelector.getMatchLabels
    // 使用标签选择器获取 Pod
    client.pods.inNamespace(podNamespace).withLabels(selector).list
  }

  def workerReplicas(): Int = {
    client.apps().statefulSets().inNamespace(podNamespace).withName(
      workerStatefulSetName).get().getSpec.getReplicas
  }

  /**
   * Scales the worker StatefulSet to the specified number of replicas
   * by updating the StatefulSet's replica count
   */
  def scaleWorkerStatefulSetReplicas(replicas: Int): Unit = {
    client.apps().statefulSets().inNamespace(podNamespace).withName(workerStatefulSetName).scale(
      replicas)
  }
}
