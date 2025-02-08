package org.apache.celeborn.service.deploy.master.scale.kubernetes

import io.fabric8.kubernetes.api.model.Pod
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.source.WorkerMetrics
import org.apache.celeborn.common.protocol.PbWorkerStatus
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.server.common.service.config.ConfigService
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager
import org.apache.celeborn.service.deploy.master.scale.{IScaleManager, ScaleOperation, ScaleRequest, ScaleType, ScalingWorker}

import scala.collection.JavaConverters._
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util

class KubernetesScaleManager(conf: CelebornConf) extends IScaleManager with Logging {

  protected val checkInterval: Long = conf.scaleCheckInterval

  protected def minWorkerNum: Int = configService.getCelebornConf.minScaleWorkerNum
  protected def maxWorkerNum: Option[Int] = configService.getCelebornConf.maxScaleWorkerNum

  protected def scaleDownEnabled: Boolean = configService.getCelebornConf.scaleDownEnabled
  protected def scaleDownDirectMemoryRatio: Double = configService.getCelebornConf.scaleDownDirectMemoryRatio
  protected def scaleDownDiskSpaceRatio: Double = configService.getCelebornConf.scaleDownDiskSpaceRatio
  protected def scaleDownCpuLoad: Double = configService.getCelebornConf.scaleDownCpuLoad
  protected def scaleDownStabilizationWindowInterval: Long = configService.getCelebornConf.scaleDownStabilizationWindowInterval
  protected def scaleDownPolicyStepNumber: Int = configService.getCelebornConf.scaleDownPolicyStepNumber
  protected def scaleDownPolicyPercent: Option[Double] = configService.getCelebornConf.scaleDownPolicyPercent

  protected def scaleUpEnabled: Boolean = configService.getCelebornConf.scaleUpEnabled
  protected def scaleUpDirectMemoryRatio: Double = configService.getCelebornConf.scaleUpDirectMemoryRatio
  protected def scaleUpDiskSpaceRatio: Double = configService.getCelebornConf.scaleUpDiskSpaceRatio
  protected def scaleUpCPULoad: Double = configService.getCelebornConf.scaleUpCPULoad
  protected def scaleUpStabilizationWindowInterval: Long = configService.getCelebornConf.scaleUpStabilizationWindowInterval
  protected def scaleUpPolicyStepNumber: Int = configService.getCelebornConf.scaleUpPolicyStepNumber
  protected def scaleUpPolicyPercent: Option[Double] = configService.getCelebornConf.scaleUpPolicyPercent

  protected val operator: KubernetesOperator = createKubernetesOperator()

  protected def createKubernetesOperator(): KubernetesOperator = new KubernetesOperatorImpl()

  protected var configService: ConfigService = _
  protected var statusSystem: AbstractMetaManager = _
  protected var scheduler: ScheduledExecutorService = _

  protected def isMasterActive: Boolean = statusSystem.isMasterActive == 1

  override def init(configService: ConfigService, statusSystem: AbstractMetaManager): Unit = {
    this.configService = configService
    this.statusSystem = statusSystem
  }

  protected def scaleUpNum(workerNum: Int): Int = {
    val num = scaleUpPolicyPercent match {
      case Some(p) => (workerNum * p).ceil.toInt
      case None => scaleUpPolicyStepNumber
    }
    if (maxWorkerNum.isEmpty) {
      num
    } else {
      Math.max(Math.min(num, maxWorkerNum.get - workerNum), 0)
    }
  }

  protected def scaleDownNum(workerNum: Int): Int = {
    val num = scaleDownPolicyPercent match {
      case Some(p) => (workerNum * p).ceil.toInt
      case None => scaleDownPolicyStepNumber
    }
    Math.max(Math.min(num, workerNum - minWorkerNum), 0)
  }

  protected def needScaleDown(avgCpuLoad: Double, avgDirectMemoryRatio: Double, avgDiskRatio: Double, workerNum: Int): Boolean = {
    if (minWorkerNum >= workerNum) {
      return false
    }

    if (avgCpuLoad < scaleDownCpuLoad && avgDiskRatio < scaleDownDiskSpaceRatio && avgDirectMemoryRatio < scaleDownDirectMemoryRatio) {
      logInfo(s"scale down, because avgDirectMemoryRatio = ${avgDirectMemoryRatio} > scaleUpDirectMemoryRatio = ${scaleUpDirectMemoryRatio}")
      return true
    }

    false
  }

  protected def needScaleUp(avgCpuLoad: Double, avgDirectMemoryRatio: Double, avgDiskRatio: Double, workerNum: Int): Boolean = {
    if (maxWorkerNum.isDefined && workerNum >= maxWorkerNum.get) {
      return false
    }

    if (avgCpuLoad > scaleUpCPULoad) {
      logInfo(s"scale up, because avgCpuLoad = ${avgCpuLoad} > scaleUpCPULoad = ${scaleUpCPULoad}")
      return true
    }

    if (avgDiskRatio > scaleUpDiskSpaceRatio) {
      logInfo(s"scale up, because avgDiskRatio = ${avgDiskRatio} > scaleUpDiskSpaceRatio = ${scaleUpDiskSpaceRatio}")
      return true
    }

    if (avgDirectMemoryRatio > scaleUpDirectMemoryRatio) {
      logInfo(s"scale up, because avgDirectMemoryRatio = ${avgDirectMemoryRatio} > scaleUpDirectMemoryRatio = ${scaleUpDirectMemoryRatio}")
      return true
    }

    false
  }

  protected def scaleType(availableWorkers: Set[WorkerInfo]): ScaleType = {
    val cpuLoads = availableWorkers.map(_.workerStatus.getStats.getOrDefault(WorkerMetrics.CPU_LOAD, "0").toDouble)
    val directMemoryRatios = availableWorkers.map(_.workerStatus.getStats.getOrDefault(WorkerMetrics.DIRECT_MEMORY_RATIO, "0").toDouble)
    val diskRatios = availableWorkers.map(_.workerStatus.getStats.getOrDefault(WorkerMetrics.DISK_RATIO, "0").toDouble)
    val avgCpuLoad = cpuLoads.sum / cpuLoads.size
    val avgDirectMemoryRatio = directMemoryRatios.sum / directMemoryRatios.size
    val avgDiskRatio = diskRatios.sum / diskRatios.size

    if (scaleUpEnabled && needScaleUp(avgCpuLoad, avgDirectMemoryRatio, avgDiskRatio, availableWorkers.size)) {
      ScaleType.SCALE_UP
    } else if (scaleDownEnabled && needScaleDown(avgCpuLoad, avgDirectMemoryRatio, avgDiskRatio, availableWorkers.size)) {
      ScaleType.SCALE_DOWN
    } else {
      ScaleType.STABILIZATION
    }
  }

  // sometimes user may manually scale replicas, check it.
  protected def checkReplicas(): Unit = {
    logInfo("check replicas")
    val podList = operator.workerPodList()
    val prevOperation = statusSystem.scaleOperation
    val newOperation = prevOperation.synchronized {
      if (prevOperation.getScaleType == ScaleType.STABILIZATION && prevOperation.getExpectedWorkerReplicaNumber != podList.getItems.size()) {
        Some(new ScaleOperation(
          prevOperation.getLastScaleUpEndTime,
          prevOperation.getLastScaleDownEndTime,
          prevOperation.getCurrentScaleStartTime,
          podList.getItems.size(),
          prevOperation.getNeedRecommissionWorkers,
          prevOperation.getNeedDecommissionWorkers,
          prevOperation.getScaleType
        ))
      } else {
        None
      }
    }
    newOperation match {
      case Some(operation) =>
        statusSystem.handleScaleOperation(operation)
        logInfo(s"The expectedWorkerReplicaNumber was changed to cluster replicas ${operation.getExpectedWorkerReplicaNumber}")
      case _ =>
    }
  }

  protected def checkPreviousScalingOperation(): Unit = {
    logInfo("check previous scaling operation")
    val workersMap = statusSystem.workersMap
    val prevOperation = statusSystem.scaleOperation
    val podList = operator.workerPodList()
    val podNameToPods = podList.getItems.asScala.map(p => (p.getMetadata.getName, p)).toMap
    var decommissionWorkers: Option[List[ScalingWorker]] = None
    var recommissionWorkers: Option[List[ScalingWorker]] = None
    val (newOperation, scaleReplicas) = prevOperation.synchronized {
      workersMap.synchronized {
        recommissionWorkers = checkRecommission(podNameToPods, workersMap, prevOperation)
        decommissionWorkers = checkDecommission(podNameToPods, workersMap, prevOperation)
      }
      if (recommissionWorkers.isEmpty && decommissionWorkers.isEmpty) {
        (None, false)
      } else {
        val r = recommissionWorkers match {
          case Some(lst) => lst
          case None => prevOperation.getNeedRecommissionWorkers.asScala
        }

        val d = decommissionWorkers match {
          case Some(lst) => lst
          case None => prevOperation.getNeedDecommissionWorkers.asScala
        }

        val (scaleType, lastScaleUpEndTime, lastScaleDownEndTime, currentScaleStartTime, scaleReplicas) = if (r.isEmpty && d.isEmpty) {
          val currentTime = System.currentTimeMillis()
          val (lastScaleUpEndTime, lastScaleDownEndTime, scaleReplicas) = if (prevOperation.getScaleType == ScaleType.SCALE_DOWN) {
            (prevOperation.getLastScaleUpEndTime, currentTime, true)
          } else {
            (currentTime, prevOperation.getLastScaleDownEndTime, !prevOperation.getNeedDecommissionWorkers.isEmpty)
          }
          (ScaleType.STABILIZATION, lastScaleUpEndTime, lastScaleDownEndTime, 0L, scaleReplicas)
        } else {
          (prevOperation.getScaleType, prevOperation.getLastScaleUpEndTime, prevOperation.getLastScaleDownEndTime, prevOperation.getCurrentScaleStartTime, false)
        }

        (Some(new ScaleOperation(
          lastScaleUpEndTime,
          lastScaleDownEndTime,
          currentScaleStartTime,
          prevOperation.getExpectedWorkerReplicaNumber,
          r.asJava,
          d.asJava,
          scaleType
        )), scaleReplicas)
      }
    }

    newOperation match {
      case Some(operation) =>
        statusSystem.handleScaleOperation(operation)
        if (scaleReplicas) {
          operator.scaleWorkerStatefulSetReplicas(operation.getExpectedWorkerReplicaNumber)
        }
      case _ =>
    }
  }
  /***
   * Remove recommissioned workers from ScaleOperation.
   * If not updated, return None
   * Otherwise, return the list of workers that are not recommissioned yet
   * */
  protected def checkRecommission(podNameToPods: Map[String, Pod], workersMap: util.Map[String, WorkerInfo], prevOperation: ScaleOperation): Option[List[ScalingWorker]] = {
    val idleWorkers = workersMap.asScala.values.filter{ worker =>
      worker.workerStatus.getState == PbWorkerStatus.State.Idle || worker.workerStatus.getState == PbWorkerStatus.State.InDecommissionThenIdle
    }.map(_.toUniqueId).toSet

    val recommissionWorkers = prevOperation.getNeedRecommissionWorkers.asScala.filter { scalingWorker =>
      podNameToPods.get(scalingWorker.getName) match {
        case Some(pod) => {
          if (scalingWorker.hasUniqueId) {
            idleWorkers.contains(scalingWorker.getUniqueId)
          } else {
            // for new created pods, now it's available
            // @TODO should we wait for normal worker status ?
            // because worker may exit after started by some unknown exception
            // if we wait for normal state, maybe we cannot finish this round
            pod.getStatus.getPhase == KubernetesOperatorImpl.POD_PHASE_PENDING
          }
        }
        case None => true
      }
    }.toList
    if (recommissionWorkers.size == prevOperation.getNeedRecommissionWorkers.size()) {
      // has not changed
      return None
    }
    Some(recommissionWorkers)
  }

  /***
   * Remove decommissioned workers from ScaleOperation.
   * If not updated, return None
   * Otherwise, return the list of workers that are not idle yet
   * */
  protected def checkDecommission(podNameToPods: Map[String, Pod], workersMap: util.Map[String, WorkerInfo], prevOperation: ScaleOperation): Option[List[ScalingWorker]] = {
    val invalidScaleState = Array(PbWorkerStatus.State.Normal, PbWorkerStatus.State.InDecommissionThenIdle, PbWorkerStatus.State.InDecommission)
    val normalIPs = workersMap.asScala.values.filter(w => invalidScaleState.contains(w.workerStatus.getState)).map(_.host).toSet

    val decommissionWorkers = prevOperation.getNeedDecommissionWorkers.asScala.filter { scalingWorker =>
      podNameToPods.get(scalingWorker.getName) match {
        case Some(pod) => {
          val ip = pod.getStatus.getPodIP
          normalIPs.contains(ip)
        }
        case None => false
      }
    }.toList

    if (decommissionWorkers.size == prevOperation.getNeedDecommissionWorkers.size()) {
      return None
    }
    Some(decommissionWorkers)
  }

  protected def tryScale(): Unit = {
    logInfo("try scale")
    val podList = operator.workerPodList()
    val podNameToPods = podList.getItems.asScala.map(p => (p.getMetadata.getName, p)).toMap

    val workersMap = statusSystem.workersMap
    val availableWorkers = statusSystem.availableWorkers
    val prevOperation = statusSystem.scaleOperation

    val currentTime = System.currentTimeMillis()

    val (newOperation, scaleReplicas) = prevOperation.synchronized {
      val (scaleType, expectedWorkerReplicaNumber) = workersMap.synchronized {
        val availableWorkerNum = availableWorkers.size()
        val availableWorkerInfos = availableWorkers.asScala.map(w => workersMap.get(w.toUniqueId)).toSet
        val scaleType = this.scaleType(availableWorkerInfos) match {
          case ScaleType.SCALE_UP =>
            if (prevOperation.getScaleType == ScaleType.SCALE_UP) {
              logInfo("The cluster is already scaling up")
              ScaleType.STABILIZATION
            } else if (scaleUpStabilizationWindowInterval > currentTime - prevOperation.getLastScaleUpEndTime) {
              logInfo("The cluster is in scale up stabilization window")
              ScaleType.STABILIZATION
            } else {
              ScaleType.SCALE_UP
            }
          case ScaleType.SCALE_DOWN =>
            if (prevOperation.getScaleType == ScaleType.SCALE_DOWN) {
              logInfo("The cluster is already scaling down")
              ScaleType.STABILIZATION
            } else if (scaleDownStabilizationWindowInterval > currentTime - prevOperation.getLastScaleDownEndTime) {
              logInfo("The cluster is in scale down stabilization window")
              ScaleType.STABILIZATION
            } else {
              ScaleType.SCALE_DOWN
            }
          case _ => ScaleType.STABILIZATION
        }
        val expectedWorkerReplicaNumber = scaleType match {
          case ScaleType.SCALE_UP => prevOperation.getExpectedWorkerReplicaNumber + scaleUpNum(availableWorkerNum)
          case ScaleType.SCALE_DOWN => prevOperation.getExpectedWorkerReplicaNumber - scaleDownNum(availableWorkerNum)
          case _ => prevOperation.getExpectedWorkerReplicaNumber
        }
        val realScaleType = if (expectedWorkerReplicaNumber > prevOperation.getExpectedWorkerReplicaNumber) {
          ScaleType.SCALE_UP
        } else if (expectedWorkerReplicaNumber < prevOperation.getExpectedWorkerReplicaNumber) {
          ScaleType.SCALE_DOWN
        } else {
          ScaleType.STABILIZATION
        }
        (realScaleType, expectedWorkerReplicaNumber)
      }

      if (scaleType == ScaleType.STABILIZATION) {
        (None, false)
      } else {
        // switch from scale down to scale up or switch from scale up to scale down.
        val (lastScaleUpEndTime, lastScaleDownEndTime) = if (scaleType == ScaleType.SCALE_UP) {
          val lastScaleDownEndTime = if (prevOperation.getScaleType == ScaleType.SCALE_DOWN) {
            currentTime
          } else {
            prevOperation.getLastScaleDownEndTime
          }
          (prevOperation.getLastScaleUpEndTime, lastScaleDownEndTime)
        } else if (scaleType == ScaleType.SCALE_DOWN) {
          val lastScaleUpEndTime = if (prevOperation.getScaleType == ScaleType.SCALE_UP) {
            currentTime
          } else {
            prevOperation.getLastScaleUpEndTime
          }
          (lastScaleUpEndTime, prevOperation.getLastScaleDownEndTime)
        } else {
          (prevOperation.getLastScaleUpEndTime, prevOperation.getLastScaleDownEndTime)
        }
        val executeScaleUp = expectedWorkerReplicaNumber > podList.getItems.size()

        val (idleWorkerUniqueIds, normalWorkerUniqueIds) = workersMap.synchronized {
          val idleWorkers = workersMap.asScala.values.filter(worker => worker.getWorkerStatus().getState == PbWorkerStatus.State.InDecommissionThenIdle || worker.getWorkerStatus().getState == PbWorkerStatus.State.Idle).map(_.toUniqueId).toList
          val normalWorkers = workersMap.asScala.values.filter(_.getWorkerStatus().getState == PbWorkerStatus.State.Normal).map(_.toUniqueId).toList
          (idleWorkers, normalWorkers)
        }

        val ipToIdleWorkers = idleWorkerUniqueIds.map(uniqueId => (WorkerInfo.fromUniqueId(uniqueId).host, uniqueId)).toMap
        val ipToNormalWorkers = normalWorkerUniqueIds.map(uniqueId => (WorkerInfo.fromUniqueId(uniqueId).host, uniqueId)).toMap

        val decommissionWorkers = (expectedWorkerReplicaNumber until podList.getItems.size()).map { idx =>
          val podName = operator.workerName(idx)
          val uniqueId = podNameToPods.get(podName) match {
            case Some(pod) => ipToNormalWorkers.getOrElse(pod.getStatus.getPodIP, null)
            case None => null
          }
          new ScalingWorker(podName, uniqueId)
        }
        val recommissionWorkers = (0 until expectedWorkerReplicaNumber).map { idx =>
          val podName = operator.workerName(idx)
          val uniqueId = podNameToPods.get(podName) match {
            case Some(pod) => ipToIdleWorkers.getOrElse(pod.getStatus.getPodIP, null)
            case None => null
          }
          new ScalingWorker(podName, uniqueId)
        }
        (Some(new ScaleOperation(
          lastScaleUpEndTime,
          lastScaleDownEndTime,
          System.currentTimeMillis(),
          expectedWorkerReplicaNumber,
          recommissionWorkers.asJava,
          decommissionWorkers.asJava,
          scaleType
        )), executeScaleUp)
      }
    }
    newOperation match {
      case Some(operation) =>
        statusSystem.handleScaleOperation(operation)
        if (scaleReplicas) {
          operator.scaleWorkerStatefulSetReplicas(operation.getExpectedWorkerReplicaNumber)
        }
      case _ =>
    }
  }

  def doScale(): Unit = {
    if (!isMasterActive) {
      return
    }
    if (!scaleUpEnabled && !scaleDownEnabled) {
      return
    }
    checkReplicas()
    checkPreviousScalingOperation()
    tryScale()
  }

  override def run(): Unit = {
    scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-scale-scheduler")
    scheduler.scheduleWithFixedDelay(
      () => {
        try {
          doScale()
        } catch {
          case e: Throwable => logError("scaling failed", e)
        }
      },
      0,
      checkInterval,
      TimeUnit.MILLISECONDS
    )
  }

  override def stop(): Unit = {
    scheduler.shutdown()
  }

  override def request(request: ScaleRequest): Long = {
    0L
  }
}
