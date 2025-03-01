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

import java.time.Clock
import java.util

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ObjectMeta, Pod, PodList, PodStatus}
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.meta.{WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.source.WorkerMetrics
import org.apache.celeborn.common.protocol.{PbWorkerStatus, WorkerEventType}
import org.apache.celeborn.server.common.service.config.{ConfigService, SystemConfig}
import org.apache.celeborn.service.deploy.master.clustermeta.{AbstractMetaManager, SingleMasterMetaManager}
import org.apache.celeborn.service.deploy.master.clustermeta.ha.{HAMasterMetaManager, HARaftServer}
import org.apache.celeborn.service.deploy.master.scale.{ScaleOperation, ScaleType}

class KubernetesScaleManagerSuite extends CelebornFunSuite with Matchers {
  private var conf: CelebornConf = _
  private var configService: ConfigService = _
  private var statusSystem: AbstractMetaManager = _
  private var kubernetesOperator: KubernetesOperator = _
  private var scaleManager: KubernetesScaleManager = _
  private var systemConfig: SystemConfig = _

  override def beforeAll(): Unit = {
    conf = new CelebornConf()
    // Set necessary configurations
    conf.set(SCALE_UP_ENABLED.key, "true")
    conf.set(SCALE_DOWN_ENABLED.key, "true")
    conf.set(MIN_SCALE_WORKER_NUM.key, "1")
    conf.set(MAX_SCALE_WORKER_NUM.key, "5")
    conf.set(SCALE_UP_CPU_LOAD.key, "70.0")
    conf.set(SCALE_UP_DIRECT_MEMORY_RATIO.key, "0.7")
    conf.set(SCALE_UP_DISK_SPACE_RATIO.key, "0.7")
    conf.set(SCALE_DOWN_CPU_LOAD.key, "30.0")
    conf.set(SCALE_DOWN_DIRECT_MEMORY_RATIO.key, "0.3")
    conf.set(SCALE_DOWN_DISK_SPACE_RATIO.key, "0.3")
    conf.set(SCALE_CHECK_INTERVAL.key, "1000")
    conf.set(SCALE_UP_POLICY_STEP_NUMBER.key, "1")
    conf.set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "1")
    conf.set(SCALE_UP_STABILIZATION_WINDOW_INTERVAL.key, "0")
    conf.set(SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL.key, "0")
  }

  override def beforeEach(): Unit = {
    conf.set(SCALE_DOWN_ENABLED.key, "true")
    conf.set(SCALE_UP_ENABLED.key, "true")
    conf.set(MIN_SCALE_WORKER_NUM.key, "1")
    conf.set(MAX_SCALE_WORKER_NUM.key, "5")
    conf.set(SCALE_UP_POLICY_STEP_NUMBER.key, "1")
    conf.set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "1")
    conf.unset(SCALE_UP_POLICY_PERCENT.key)
    conf.unset(SCALE_DOWN_POLICY_PERCENT.key)
    // Mock dependencies
    configService = mock[ConfigService]()
    kubernetesOperator = mock[TestKubernetesOperator]()
    systemConfig = new SystemConfig(conf)
    when(kubernetesOperator.workerName(anyInt)).thenCallRealMethod()
    when(configService.getSystemConfigFromCache).thenReturn(systemConfig)
    when(configService.getCelebornConf).thenReturn(conf)
    // Create scale manager with mocked operator
    statusSystem = new SingleMasterMetaManager(null, conf)
    scaleManager = new TestKubernetesScaleManager(conf)
    scaleManager.init(configService, statusSystem)
  }

  test("should scale up when resource usage is high") {
    // Create mock workers with high resource usage
    var workers = createWorkers(
      1,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.5",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.5"))

    setupWorkerMocks(workers)
    var podList = createPodList(1)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale check
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
    // Verify no need to update replica
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(2)
    doAnswer(_ => 2).when(kubernetesOperator).workerReplicas()
    // haven't finished yet
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2

    podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()
    workerHeartBeat(workers)
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
  }

  test("should scale up when disk ratio exceeds threshold") {
    // Create mock workers with only high disk usage
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "30.0", // Low CPU load
        WorkerMetrics.DISK_RATIO -> "0.9", // High disk usage
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.3" // Low memory usage
      ))

    setupWorkerMocks(workers)
    val podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale check
    scaleManager.doScale()

    // Should scale up due to high disk usage, even though other metrics are low
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
  }

  test("should scale up when direct memory ratio exceeds threshold") {
    // Create mock workers with only high memory usage
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "0.3", // Low CPU load
        WorkerMetrics.DISK_RATIO -> "0.3", // Low disk usage
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.9" // High memory usage
      ))

    setupWorkerMocks(workers)
    val podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale check
    scaleManager.doScale()

    // Should scale up due to high memory usage, even though other metrics are low
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
  }

  test("should scale down when all metrics are below threshold") {
    // Create mock workers with only low disk usage
    var workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "50.0", // Medium CPU load
        WorkerMetrics.DISK_RATIO -> "0.2", // Low disk usage
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.5" // Medium memory usage
      ))

    setupWorkerMocks(workers)
    val podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale check
    scaleManager.doScale()

    // Should not scale down due to medium CPU load and Medium memory usage
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3

    workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "50.0", // Medium CPU load
        WorkerMetrics.DISK_RATIO -> "0.2", // Low disk usage
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2" // Low memory usage
      ))
    setupWorkerMocks(workers)

    // Trigger scale check
    scaleManager.doScale()

    // Should not scale down due to medium CPU load
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3

    workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0", // Medium CPU load
        WorkerMetrics.DISK_RATIO -> "0.2", // Low disk usage
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2" // Low memory usage
      ))
    setupWorkerMocks(workers)
    // Trigger scale check
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
  }

  test("should switch from scale up to scale down when resource usage becomes low") {
    // Create mock workers with high resource usage
    var workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    var podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger initial scale up
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    doAnswer(_ => 3).when(kubernetesOperator).workerReplicas()

    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
    clearInvocations(kubernetesOperator)
    // Change to low resource usage
    workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))
    setupWorkerMocks(workers)
    podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale check again, should switch to scale down
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
    val event = statusSystem.workerEventInfos.get(WorkerInfo.fromUniqueId(workers(2).toUniqueId))
    event should not be null
    event.getEventType should be(WorkerEventType.DecommissionThenIdle)
    // cannot update replicas because worker has not been decommissioned.
    scaleManager.doScale()
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // still cannot update replicas
    markWorker(workers(2), PbWorkerStatus.State.InDecommissionThenIdle_VALUE)
    workerHeartBeat(workers)
    scaleManager.doScale()
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // now it's safe to update replicas
    markWorker(workers(2), PbWorkerStatus.State.Idle_VALUE)
    workerHeartBeat(workers)
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(2)

  }

  test("should switch from scale down to scale up when resource usage becomes high") {
    // Create mock workers with low resource usage
    val workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))

    setupWorkerMocks(workers)
    var podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger initial scale down
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
    scaleManager.doScale()
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    markWorker(workers(2), PbWorkerStatus.State.Idle_VALUE)
    workerHeartBeat(workers)
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(2)

    // Change to high resource usage
    val updatedWorkers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))
    setupWorkerMocks(updatedWorkers)

    // Mock updated pod list after scale down
    podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()
    // Trigger scale check again, should switch to scale up
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
  }

  test("should not scale up when reaching max worker limit") {
    // Create mock workers with high resource usage
    val workers = createWorkers(
      5,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    val podList = createPodList(5)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Try to scale up
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 5
    scaleManager.doScale()
    // Verify no scale operation was triggered since we're at max
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)
  }

  test("should not scale down when reaching min worker limit") {
    // Create mock workers with low resource usage
    val workers = createWorkers(
      1,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))

    setupWorkerMocks(workers)
    val podList = createPodList(1)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Try to scale down
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 1
    markWorker(workers.head, PbWorkerStatus.State.Idle_VALUE)
    workerHeartBeat(workers)
    scaleManager.doScale()
    // Verify no scale operation was triggered since we're at min
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)
  }

  test("should respect min worker limits during scaling") {
    conf.set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "2")
    // Change to low resource usage
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "10.0",
        WorkerMetrics.DISK_RATIO -> "0.1",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.1"))
    setupWorkerMocks(workers)
    // Update pod list to show max workers
    val podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()
    // Trigger scale down
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 1
    scaleManager.doScale()
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)
    markWorker(workers(1), PbWorkerStatus.State.Idle_VALUE)
    workerHeartBeat(workers)
    scaleManager.doScale()
    // Verify scale down respects min limit (1)
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(1)
  }

  test("should respect max worker limits during scaling") {

    conf.set(SCALE_UP_POLICY_STEP_NUMBER.key, "2")

    // Create mock workers with high resource usage
    val workers = createWorkers(
      4,
      Map(
        WorkerMetrics.CPU_LOAD -> "90.0",
        WorkerMetrics.DISK_RATIO -> "0.9",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.9"))

    setupWorkerMocks(workers)
    val podList = createPodList(4)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale up
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 5
    // Verify scale up respects max limit (5)
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(5)
  }

  test("should retry when kubernetes scale operation fails") {
    // Create mock workers with high resource usage
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    val podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Mock scale operation failure
    when(kubernetesOperator.scaleWorkerStatefulSetReplicas(3))
      .thenThrow(new RuntimeException("Scale operation failed"))

    // First attempt - should fail but not throw exception
    assertThrows[RuntimeException] {
      scaleManager.doScale()
    }
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
    doNothing().when(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
    clearInvocations(kubernetesOperator)
    // Second attempt - should retry and succeed
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
  }

  test("should retry when kubernetes scale operation does not take effect") {
    // Create mock workers with high resource usage
    var workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    var podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // First scale attempt
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
    clearInvocations(kubernetesOperator)

    // Pod list still shows original count (scale operation didn't take effect)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Second attempt - should retry since desired state wasn't reached
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
    clearInvocations(kubernetesOperator)

    // Pod list still not updated
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Third attempt - should continue retrying
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
    clearInvocations(kubernetesOperator)

    // Finally, pod list shows the scale operation took effect
    podList = createPodList(3)
    workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "50.0",
        WorkerMetrics.DISK_RATIO -> "0.5",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.5"))
    setupWorkerMocks(workers)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // No more retries needed
    scaleManager.doScale()
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt())
  }

  test("should handle intermittent kubernetes api failures") {
    // Create mock workers with high resource usage
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)

    // Mock intermittent pod list failures
    when(kubernetesOperator.workerPodList())
      .thenThrow(new RuntimeException("API server unavailable"))
      .thenReturn(createPodList(2))
    doAnswer(_ => 2).when(kubernetesOperator).workerReplicas()

    // First attempt - should handle API failure gracefully
    assertThrows[RuntimeException] {
      scaleManager.doScale()
    }
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // Second attempt - API works, should proceed with scaling
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
  }

  test("should handle worker recommission during scale down") {
    // Create mock workers with low resource usage
    val workers = createWorkers(
      4,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))

    // Set up initial pod list with 4 workers
    setupWorkerMocks(workers)
    val podList = createPodList(4)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()
    // Trigger scale down
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    scaleManager.doScale()
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)
    markWorker(workers(3), PbWorkerStatus.State.InDecommissionThenIdle_VALUE)

    // Update metrics to show high resource usage
    for (worker <- workers) {
      worker.workerStatus.getStats.put(WorkerMetrics.CPU_LOAD, "80.0")
      worker.workerStatus.getStats.put(WorkerMetrics.DISK_RATIO, "0.8")
      worker.workerStatus.getStats.put(WorkerMetrics.DIRECT_MEMORY_RATIO, "0.8")
    }
    workerHeartBeat(workers)
    // Trigger scale check again
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 4
    statusSystem.scaleOperation.getNeedRecommissionWorkers.size() shouldEqual 4

    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 4
    statusSystem.scaleOperation.getNeedRecommissionWorkers.size() shouldEqual 1
    val r = statusSystem.scaleOperation.getNeedRecommissionWorkers.get(0)
    r.getUniqueId shouldEqual workers(3).toUniqueId
    r.getName shouldEqual podList.getItems.get(3).getMetadata.getName
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)
  }

  test("should only scale up when scale down is disabled") {
    // Disable scale down, only enable scale up
    conf.set(SCALE_DOWN_ENABLED.key, "false")
    conf.set(SCALE_UP_ENABLED.key, "true")

    // Create mock workers with low resource usage
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))

    setupWorkerMocks(workers)
    val podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Try to scale down - should not trigger scale operation
    scaleManager.doScale()
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // Update to high resource usage
    val highLoadWorkers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))
    setupWorkerMocks(highLoadWorkers)

    // Should trigger scale up even though scale down is disabled
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
  }

  test("should only scale down when scale up is disabled") {
    // Disable scale up, only enable scale down
    conf.set(SCALE_UP_ENABLED.key, "false")
    conf.set(SCALE_DOWN_ENABLED.key, "true")

    // Create mock workers with high resource usage
    val workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    val podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Try to scale up - should not trigger scale operation
    scaleManager.doScale()
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // Update to low resource usage
    val lowLoadWorkers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))
    setupWorkerMocks(lowLoadWorkers)

    // Should trigger scale down even though scale up is disabled
    scaleManager.doScale()
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
  }

  test("should scale down by percentage") {
    conf.set(SCALE_DOWN_POLICY_PERCENT.key, "0.3") // 30% decrease

    // Create mock workers with high resource usage
    val workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))

    setupWorkerMocks(workers)
    val podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale up
    scaleManager.doScale()
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2
  }

  test("should scale up by percentage") {
    // Configure percentage based scaling
    conf.set(SCALE_UP_POLICY_PERCENT.key, "0.3") // 30% increase

    // Create mock workers with high resource usage
    val workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    val podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale up
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(4)
  }

  test("Scale up should respect stabilization interval") {
    val intervalSeconds = 60000
    val startTime = System.currentTimeMillis()
    conf.set(
      SCALE_UP_STABILIZATION_WINDOW_INTERVAL.key,
      intervalSeconds.toString
    ) // 1 minute window

    val mockClock = mock[Clock]()
    scaleManager = new TestKubernetesScaleManager(conf, Some(mockClock))
    scaleManager.init(configService, statusSystem)

    // Create mock workers with high resource usage
    var workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    var podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    when(mockClock.millis).thenReturn(startTime)
    // Trigger scale check
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)
    clearInvocations(kubernetesOperator)

    // Create mock workers with high resource usage
    workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Move time forward but still within stabilization window
    when(mockClock.millis()).thenReturn(startTime + 30000) // 30 seconds later

    // Try to scale up again - should be blocked by stabilization window
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // Move time forward past stabilization window
    when(mockClock.millis()).thenReturn(startTime + 100000) // 100 seconds later

    // Now should allow new scale up
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    verify(kubernetesOperator, times(1)).scaleWorkerStatefulSetReplicas(4)
  }

  test("Scale down should respect stabilization interval") {
    val intervalSeconds = 60000
    conf.set(
      SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL.key,
      intervalSeconds.toString
    ) // 1 minute window

    val mockClock = mock[Clock]()
    val startTime = System.currentTimeMillis()
    when(mockClock.millis()).thenReturn(startTime)

    scaleManager = new TestKubernetesScaleManager(conf, Some(mockClock))
    scaleManager.init(configService, statusSystem)

    // Create mock workers with low resource usage
    var workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))

    setupWorkerMocks(workers)
    var podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // First scale down
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2

    // Mark worker as idle and complete scale down
    markWorker(workers(2), PbWorkerStatus.State.Idle_VALUE)
    workerHeartBeat(workers)
    scaleManager.doScale()
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(2)
    clearInvocations(kubernetesOperator)

    podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()
    workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "20.0",
        WorkerMetrics.DISK_RATIO -> "0.2",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.2"))
    setupWorkerMocks(workers)
    // Move time forward but still within stabilization window
    when(mockClock.millis()).thenReturn(startTime + 30000) // 30 seconds later
    // Try to scale down again - should be blocked by stabilization window
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // Move time forward past stabilization window
    when(mockClock.millis()).thenReturn(startTime + 100000) // 100 seconds later

    // Now should allow new scale down
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
  }

  test("should consider scale up successful when worker exits abnormally but pod count matches") {
    val intervalSeconds = 60000
    conf.set(
      SCALE_UP_STABILIZATION_WINDOW_INTERVAL.key,
      intervalSeconds.toString
    ) // 1 minute window
    // Create mock workers with high resource usage to trigger scale up
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    var podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // Trigger scale up
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_UP
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
    verify(kubernetesOperator).scaleWorkerStatefulSetReplicas(3)

    // Update pod list to show 3 pods (scale up succeeded at pod level)
    podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // But worker process exits abnormally - worker count stays at 2
    // We don't add the new worker to workers list to simulate worker process exit
    setupWorkerMocks(workers) // Still only 2 workers

    // Check scale status - should be STABILIZATION since pod count matches expected
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.STABILIZATION
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 3
  }

  test("should not scale before worker heartbeat timeout") {
    val mockClock = mock[Clock]()
    val startTime = System.currentTimeMillis()
    when(mockClock.millis).thenReturn(startTime)
    val haStatusSystem = spy[HAMasterMetaManager](new HAMasterMetaManager(null, conf))
    val ratisServer = mock[HARaftServer]()
    when(haStatusSystem.getRatisServer).thenReturn(ratisServer)
    when(ratisServer.isLeader).thenReturn(true)
    when(ratisServer.getWorkerTimeoutDeadline).thenReturn(startTime + 1000)
    doNothing().when(haStatusSystem).handleScaleOperation(any(classOf[ScaleOperation]))
    doNothing().when(haStatusSystem).handleUpdateReplicas(anyInt)
    statusSystem = haStatusSystem
    scaleManager = new TestKubernetesScaleManager(conf, Some(mockClock))
    scaleManager.init(configService, statusSystem)
    // Create mock workers with high resource usage that would normally trigger scale up
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    val podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    scaleManager.doScale()
    verify(haStatusSystem, never()).handleScaleOperation(any())

    when(mockClock.millis).thenReturn(startTime + 2000)
    scaleManager.doScale()

    verify(haStatusSystem, atLeastOnce()).handleScaleOperation(any())
  }

  test("should only scale when node is master") {
    val mockClock = mock[Clock]()
    val startTime = System.currentTimeMillis()
    when(mockClock.millis).thenReturn(startTime)

    // Create HA status system with Ratis server
    val haStatusSystem = spy[HAMasterMetaManager](new HAMasterMetaManager(null, conf))
    val ratisServer = mock[HARaftServer]()
    when(haStatusSystem.getRatisServer).thenReturn(ratisServer)
    when(ratisServer.getWorkerTimeoutDeadline).thenReturn(startTime)
    doNothing().when(haStatusSystem).handleScaleOperation(any(classOf[ScaleOperation]))
    doNothing().when(haStatusSystem).handleUpdateReplicas(anyInt)

    statusSystem = haStatusSystem
    scaleManager = new TestKubernetesScaleManager(conf, Some(mockClock))
    scaleManager.init(configService, statusSystem)

    // Create mock workers with high resource usage that would normally trigger scale up
    val workers = createWorkers(
      2,
      Map(
        WorkerMetrics.CPU_LOAD -> "80.0",
        WorkerMetrics.DISK_RATIO -> "0.8",
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.8"))

    setupWorkerMocks(workers)
    val podList = createPodList(2)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    // First try when not master
    when(ratisServer.isLeader).thenReturn(false)
    scaleManager.doScale()
    verify(haStatusSystem, never()).handleScaleOperation(any())
    verify(kubernetesOperator, never()).scaleWorkerStatefulSetReplicas(anyInt)

    // Now try as master
    when(ratisServer.isLeader).thenReturn(true)
    scaleManager.doScale()
    verify(haStatusSystem, atLeastOnce()).handleScaleOperation(any())
  }

  test("should adjust scale down target based on predicted load") {
    // Create mock workers with medium-low resource usage
    val workers = createWorkers(
      3,
      Map(
        WorkerMetrics.CPU_LOAD -> "10.0", // Medium-low CPU load
        WorkerMetrics.DISK_RATIO -> "0.1", // Medium-low disk usage
        WorkerMetrics.DIRECT_MEMORY_RATIO -> "0.1" // Medium-low memory usage
      ))
    workers.head.workerStatus.getStats.put(WorkerMetrics.CPU_LOAD, "80.0")

    setupWorkerMocks(workers)
    val podList = createPodList(3)
    when(kubernetesOperator.workerPodList()).thenReturn(podList)
    doAnswer(_ => podList.getItems.size()).when(kubernetesOperator).workerReplicas()

    conf.set(SCALE_DOWN_POLICY_STEP_NUMBER.key, "2")
    conf.set(SCALE_DOWN_CPU_LOAD.key, "40.0")

    // But predicted load would be too high with just 1 worker:
    // Current load per worker: CPU=33.3%
    // With 1 worker: CPU=100% (above scale up thresholds)
    // With 2 workers: CPU=10% (below scale up thresholds)
    // With 3 workers: CPU=10% (below scale up thresholds)

    // Trigger scale check - should scale down to 2 workers instead of 1
    scaleManager.doScale()
    statusSystem.scaleOperation.getScaleType shouldEqual ScaleType.SCALE_DOWN
    statusSystem.scaleOperation.getExpectedWorkerReplicaNumber shouldEqual 2

  }

  private def createWorkers(count: Int, metrics: Map[String, String]): Seq[WorkerInfo] = {
    (0 until count).map { i =>
      val worker = new WorkerInfo(s"1.1.1.${i}", -1, -1, -1, -1)
      val status = new WorkerStatus(PbWorkerStatus.State.Normal_VALUE, System.currentTimeMillis())
      val stats = status.getStats
      metrics.foreach { case (k, v) => stats.put(k, v) }
      worker.workerStatus = status
      worker
    }
  }

  private def setupWorkerMocks(workers: Seq[WorkerInfo]): Unit = {
    statusSystem.workersMap.clear()
    workers.foreach(w => statusSystem.workersMap.put(w.toUniqueId, w))
    statusSystem.availableWorkers.clear()
    statusSystem.availableWorkers.addAll(workers.map(w =>
      WorkerInfo.fromUniqueId(w.toUniqueId)).asJava)
  }

  private def createPodList(size: Int): PodList = {
    val podList = mock[PodList]()
    val pods = (0 until size).map { idx =>
      val pod = mock[Pod]()
      val objectMeta = mock[ObjectMeta]()
      val ip = s"1.1.1.${idx}"
      val name = s"celeborn-worker-${idx}"
      val status = mock[PodStatus]()
      when(status.getPodIP).thenReturn(ip)
      when(pod.getStatus).thenReturn(status)
      when(pod.getMetadata).thenReturn(objectMeta)
      when(objectMeta.getName).thenReturn(name)
      pod
    }.toList
    when(podList.getItems).thenReturn(pods.asJava)
    podList
  }

  def markWorker(worker: WorkerInfo, state: Int): Unit = {
    worker.setWorkerStatus(new WorkerStatus(
      state,
      System.currentTimeMillis(),
      new util.HashMap[String, String]()))
  }

  def workerHeartBeat(workers: Seq[WorkerInfo]): Unit = {
    for (worker <- workers) {
      val time = System.currentTimeMillis()
      statusSystem.handleWorkerHeartbeat(
        worker.host,
        worker.rpcPort,
        worker.pushPort,
        worker.fetchPort,
        worker.replicatePort,
        worker.diskInfos,
        worker.userResourceConsumption,
        time,
        false,
        worker.getWorkerStatus(),
        time.toString)
    }
  }

  // Test implementation that uses mocked operator
  private class TestKubernetesScaleManager(conf: CelebornConf, mockClock: Option[Clock] = None)
    extends KubernetesScaleManager(conf) {
    override protected def createKubernetesOperator(): KubernetesOperator = kubernetesOperator

    override def clock: Clock =
      if (mockClock.isEmpty) {
        super.clock
      } else {
        mockClock.get
      }
  }

  private trait TestKubernetesOperator extends KubernetesOperator {
    def workerName(idx: Int) = s"celeborn-worker-${idx}"
  }
}
