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

/**
 * Trait defining operations for managing Celeborn workers in a Kubernetes environment.
 * This interface provides methods to interact with and manage worker pods and StatefulSets.
 */
trait KubernetesOperator {
  /**
   * Retrieves the list of all worker pods currently running in the Kubernetes cluster.
   *
   * @return PodList containing information about all worker pods
   */
  def workerPodList(): PodList

  /**
   * Scales the worker StatefulSet to the specified number of replicas.
   *
   * @param replicas The desired number of worker pod replicas
   */
  def scaleWorkerStatefulSetReplicas(replicas: Int): Unit

  /**
   * Generates the name for a worker pod at the specified index.
   *
   * @param idx The index of the worker pod
   * @return The name of the worker pod
   */
  def workerName(idx: Int): String
}
