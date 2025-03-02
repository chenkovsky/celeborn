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

import scala.collection.JavaConverters._

import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.server.common.service.config.{ConfigService, SystemConfig}
import org.apache.celeborn.service.deploy.master.clustermeta.SingleMasterMetaManager

class ConfigServiceSuite extends CelebornFunSuite with Matchers {

  test("test dynamic configuration with null ConfigService") {
    val conf = new CelebornConf()
    // Worker count limits
    conf.set(CelebornConf.MIN_SCALE_WORKER_NUM, 2)
    conf.set(CelebornConf.MAX_SCALE_WORKER_NUM, 10)

    // Scale down configuration
    conf.set(CelebornConf.SCALE_DOWN_ENABLED, true)
    conf.set(CelebornConf.SCALE_DOWN_DIRECT_MEMORY_RATIO, 0.8)
    conf.set(CelebornConf.SCALE_DOWN_DISK_SPACE_RATIO, 0.7)
    conf.set(CelebornConf.SCALE_DOWN_CPU_LOAD, 0.6)
    conf.set(CelebornConf.SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL, 60000L)
    conf.set(CelebornConf.SCALE_DOWN_POLICY_STEP_NUMBER, 1)
    conf.set(CelebornConf.SCALE_DOWN_POLICY_PERCENT, 0.2)

    // Scale up configuration
    conf.set(CelebornConf.SCALE_UP_ENABLED, true)
    conf.set(CelebornConf.SCALE_UP_DIRECT_MEMORY_RATIO, 0.7)
    conf.set(CelebornConf.SCALE_UP_DISK_SPACE_RATIO, 0.8)
    conf.set(CelebornConf.SCALE_UP_CPU_LOAD, 0.9)
    conf.set(CelebornConf.SCALE_UP_STABILIZATION_WINDOW_INTERVAL, 30000L)
    conf.set(CelebornConf.SCALE_UP_POLICY_STEP_NUMBER, 2)
    conf.set(CelebornConf.SCALE_UP_POLICY_PERCENT, 0.3)

    val manager = new KubernetesScaleManager(conf)
    val statusSystem = new SingleMasterMetaManager(null, conf)
    manager.init(null, statusSystem)

    // Test worker count limits
    assert(manager.minWorkerNum === 2)
    assert(manager.maxWorkerNum.contains(10))

    // Test scale down configuration
    assert(manager.scaleDownEnabled === true)
    assert(manager.scaleDownDirectMemoryRatio === 0.8)
    assert(manager.scaleDownDiskSpaceRatio === 0.7)
    assert(manager.scaleDownCpuLoad === 0.6)
    assert(manager.scaleDownStabilizationWindowInterval === 60000L)
    assert(manager.scaleDownPolicyStepNumber === 1)
    assert(manager.scaleDownPolicyPercent.contains(0.2))

    // Test scale up configuration
    assert(manager.scaleUpEnabled === true)
    assert(manager.scaleUpDirectMemoryRatio === 0.7)
    assert(manager.scaleUpDiskSpaceRatio === 0.8)
    assert(manager.scaleUpCPULoad === 0.9)
    assert(manager.scaleUpStabilizationWindowInterval === 30000L)
    assert(manager.scaleUpPolicyStepNumber === 2)
    assert(manager.scaleUpPolicyPercent.contains(0.3))
  }

  test("test dynamic configuration with ConfigService") {
    val conf = new CelebornConf()
    // Set default values in CelebornConf
    // Worker count limits
    conf.set(CelebornConf.MIN_SCALE_WORKER_NUM, 2)
    conf.set(CelebornConf.MAX_SCALE_WORKER_NUM, 10)

    // Scale down configuration
    conf.set(CelebornConf.SCALE_DOWN_ENABLED, false)
    conf.set(CelebornConf.SCALE_DOWN_DIRECT_MEMORY_RATIO, 0.8)
    conf.set(CelebornConf.SCALE_DOWN_DISK_SPACE_RATIO, 0.7)
    conf.set(CelebornConf.SCALE_DOWN_CPU_LOAD, 0.6)
    conf.set(CelebornConf.SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL, 60000L)
    conf.set(CelebornConf.SCALE_DOWN_POLICY_STEP_NUMBER, 1)
    conf.set(CelebornConf.SCALE_DOWN_POLICY_PERCENT, 0.2)

    // Scale up configuration
    conf.set(CelebornConf.SCALE_UP_ENABLED, false)
    conf.set(CelebornConf.SCALE_UP_DIRECT_MEMORY_RATIO, 0.7)
    conf.set(CelebornConf.SCALE_UP_DISK_SPACE_RATIO, 0.8)
    conf.set(CelebornConf.SCALE_UP_CPU_LOAD, 0.9)
    conf.set(CelebornConf.SCALE_UP_STABILIZATION_WINDOW_INTERVAL, 30000L)
    conf.set(CelebornConf.SCALE_UP_POLICY_STEP_NUMBER, 2)
    conf.set(CelebornConf.SCALE_UP_POLICY_PERCENT, 0.3)

    val manager = new KubernetesScaleManager(conf)

    // Mock ConfigService
    val configService = mock(classOf[ConfigService])
    val systemConfig = new SystemConfig(conf)

    // Set all dynamic config values
    systemConfig.setConfigs(Map(
      // Worker count limits
      CelebornConf.MIN_SCALE_WORKER_NUM.key -> "3",
      CelebornConf.MAX_SCALE_WORKER_NUM.key -> "20",

      // Scale down configuration
      CelebornConf.SCALE_DOWN_ENABLED.key -> "true",
      CelebornConf.SCALE_DOWN_DIRECT_MEMORY_RATIO.key -> "0.9",
      CelebornConf.SCALE_DOWN_DISK_SPACE_RATIO.key -> "0.8",
      CelebornConf.SCALE_DOWN_CPU_LOAD.key -> "0.7",
      CelebornConf.SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL.key -> "120s",
      CelebornConf.SCALE_DOWN_POLICY_STEP_NUMBER.key -> "2",
      CelebornConf.SCALE_DOWN_POLICY_PERCENT.key -> "0.25",

      // Scale up configuration
      CelebornConf.SCALE_UP_ENABLED.key -> "true",
      CelebornConf.SCALE_UP_DIRECT_MEMORY_RATIO.key -> "0.8",
      CelebornConf.SCALE_UP_DISK_SPACE_RATIO.key -> "0.9",
      CelebornConf.SCALE_UP_CPU_LOAD.key -> "0.95",
      CelebornConf.SCALE_UP_STABILIZATION_WINDOW_INTERVAL.key -> "45s",
      CelebornConf.SCALE_UP_POLICY_STEP_NUMBER.key -> "3",
      CelebornConf.SCALE_UP_POLICY_PERCENT.key -> "0.35").asJava)

    // Setup dynamic config values
    when(configService.getSystemConfigFromCache).thenReturn(systemConfig)
    when(configService.getCelebornConf).thenReturn(conf)

    manager.init(configService, null)

    // Verify worker count limits
    assert(manager.minWorkerNum === 3)
    assert(manager.maxWorkerNum.contains(20))

    // Verify scale down configuration
    assert(manager.scaleDownEnabled === true)
    assert(manager.scaleDownDirectMemoryRatio === 0.9)
    assert(manager.scaleDownDiskSpaceRatio === 0.8)
    assert(manager.scaleDownCpuLoad === 0.7)
    assert(manager.scaleDownStabilizationWindowInterval === 120000L)
    assert(manager.scaleDownPolicyStepNumber === 2)
    assert(manager.scaleDownPolicyPercent.contains(0.25))

    // Verify scale up configuration
    assert(manager.scaleUpEnabled === true)
    assert(manager.scaleUpDirectMemoryRatio === 0.8)
    assert(manager.scaleUpDiskSpaceRatio === 0.9)
    assert(manager.scaleUpCPULoad === 0.95)
    assert(manager.scaleUpStabilizationWindowInterval === 45000L)
    assert(manager.scaleUpPolicyStepNumber === 3)
    assert(manager.scaleUpPolicyPercent.contains(0.35))
  }

  test("test empty optional configuration values") {
    val conf = new CelebornConf()
    val manager = new KubernetesScaleManager(conf)

    // Test empty optional values when ConfigService is null
    assert(manager.maxWorkerNum.isEmpty)
    assert(manager.scaleDownPolicyPercent.isEmpty)
    assert(manager.scaleUpPolicyPercent.isEmpty)

    // Mock ConfigService with empty optional values
    val configService = mock(classOf[ConfigService])
    val systemConfig = new SystemConfig(conf)

    // Set empty values for optional configs
    systemConfig.setConfigs(Map(
      CelebornConf.MAX_SCALE_WORKER_NUM.key -> "",
      CelebornConf.SCALE_DOWN_POLICY_PERCENT.key -> "",
      CelebornConf.SCALE_UP_POLICY_PERCENT.key -> "").asJava)

    when(configService.getSystemConfigFromCache).thenReturn(systemConfig)
    when(configService.getCelebornConf).thenReturn(conf)

    manager.init(configService, null)

    // Verify optional values remain empty
    assert(manager.maxWorkerNum.isEmpty)
    assert(manager.scaleDownPolicyPercent.isEmpty)
    assert(manager.scaleUpPolicyPercent.isEmpty)
  }

  // Test implementation that uses mocked operator
  private class TestKubernetesScaleManager(conf: CelebornConf)
    extends KubernetesScaleManager(conf) {
    override protected def createKubernetesOperator(): KubernetesOperator =
      mock[TestKubernetesOperator]()
  }

  private trait TestKubernetesOperator extends KubernetesOperator {
    def workerName(idx: Int) = s"celeborn-worker-${idx}"
  }
}
