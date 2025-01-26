package org.apache.celeborn.service.deploy.master.scale.kubernetes

import io.fabric8.kubernetes.api.model.Pod
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.source.WorkerMetrics
import org.apache.celeborn.common.protocol.PbWorkerStatus
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.master.scale.{IScaleManager, ScaleOperation, ScaleType, ScalingWorker}

import scala.collection.JavaConverters._
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util

class KubernetesScaleManager(conf: CelebornConf) extends IScaleManager with Logging {

  protected val checkInterval: Long = conf.scaleCheckInterval
  protected val minWorkerNum: Int = conf.minScaleWorkerNum
  protected val maxWorkerNum: Option[Int] = conf.maxScaleWorkerNum

  protected val scaleDownEnabled: Boolean = conf.scaleDownEnabled
  protected val scaleDownDirectMemoryRatio: Double = conf.scaleDownDirectMemoryRatio
  protected val scaleDownDiskSpaceRatio: Double = conf.scaleDownDiskSpaceRatio
  protected val scaleDownCpuLoad: Double = conf.scaleDownCpuLoad
  protected val scaleDownStabilizationWindowInterval: Long = conf.scaleDownStabilizationWindowInterval
  protected val scaleDownPolicyStepNumber: Int = conf.scaleDownPolicyStepNumber
  protected val scaleDownPolicyPercent: Option[Double] = conf.scaleDownPolicyPercent

  protected val scaleUpEnabled: Boolean = conf.scaleUpEnabled
  protected val scaleUpDirectMemoryRatio: Double = conf.scaleUpDirectMemoryRatio
  protected val scaleUpDiskSpaceRatio: Double = conf.scaleUpDiskSpaceRatio
  protected val scaleUpCPULoad: Double = conf.scaleUpCPULoad
  protected val scaleUpStabilizationWindowInterval: Long = conf.scaleUpStabilizationWindowInterval
  protected val scaleUpPolicyStepNumber: Int = conf.scaleUpPolicyStepNumber
  protected val scaleUpPolicyPercent: Option[Double] = conf.scaleUpPolicyPercent

  protected val operator = new KubernetesOperator()
  protected var master: Master = _
  protected var scheduler: ScheduledExecutorService = _

  protected def isMasterActive = master.isMasterActive == 1

  override def init(master: Master): Unit = {
    this.master = master
  }

  protected def scaleUpNum(workerNum: Int): Int = {
    val num = scaleUpPolicyPercent match {
      case Some(p) => (workerNum * p).toInt
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
      case Some(p) => (workerNum * p).toInt
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
    }
    ScaleType.STABILIZATION
  }

  protected def checkPreviousScalingOperation(): Unit = {
    val workersMap = master.statusSystem.workersMap
    val prevOperation = master.statusSystem.scaleOperation
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
        master.statusSystem.handleScaleOperation(operation)
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
    }.map(_.toUniqueId()).toSet

    val recommissionWorkers = prevOperation.getNeedRecommissionWorkers.asScala.filter { scalingWorker =>
      podNameToPods.get(scalingWorker.getName) match {
        case Some(pod) => {
          if (scalingWorker.hasUniqueId) {
            idleWorkers.contains(scalingWorker.getUniqueId)
          } else {
            // for new created pods, now it's available
            pod.getStatus.getPhase != KubernetesOperator.POD_PHASE_PENDING
          }
        }
        case None => true
      }
    }.toList
    if (recommissionWorkers.size == prevOperation.getNeedRecommissionWorkers.size()) {
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
    val normalIPs = workersMap.asScala.values.filter(_.workerStatus.getState == PbWorkerStatus.State.Normal).map(_.host).toSet

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

  protected def tryScale() = {
    val podList = operator.workerPodList()
    val podNameToPods = podList.getItems.asScala.map(p => (p.getMetadata.getName, p)).toMap

    val workersMap = master.statusSystem.workersMap
    val availableWorkers = master.statusSystem.availableWorkers
    val prevOperation = master.statusSystem.scaleOperation

    val currentTime = System.currentTimeMillis()

    val (newOperation, scaleReplicas) = prevOperation.synchronized {
      val (scaleType, expectedWorkerReplicaNumber) = workersMap.synchronized {
        val availableWorkerNum = availableWorkers.size()
        val availableWorkerInfos = availableWorkers.asScala.map(w => workersMap.get(w.toUniqueId())).toSet
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
          case ScaleType.SCALE_UP => prevOperation.getExpectedWorkerReplicaNumber +  scaleUpNum(availableWorkerNum)
          case ScaleType.SCALE_DOWN => prevOperation.getExpectedWorkerReplicaNumber - scaleDownNum(availableWorkerNum)
          case _ => prevOperation.getExpectedWorkerReplicaNumber
        }
        val realScaleType =  if (expectedWorkerReplicaNumber > prevOperation.getExpectedWorkerReplicaNumber) {
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
          val idleWorkers = workersMap.asScala.values.filter(worker => worker.getWorkerStatus().getState == PbWorkerStatus.State.InDecommissionThenIdle || worker.getWorkerStatus().getState == PbWorkerStatus.State.Idle).map(_.toUniqueId()).toList
          val normalWorkers = workersMap.asScala.values.filter(_.getWorkerStatus().getState == PbWorkerStatus.State.Normal).map(_.toUniqueId()).toList
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
          prevOperation.getExpectedWorkerReplicaNumber,
          recommissionWorkers.asJava,
          decommissionWorkers.asJava,
          scaleType
        )), executeScaleUp)
      }
    }
    newOperation match {
      case Some(operation) =>
        master.statusSystem.handleScaleOperation(operation)
        if (scaleReplicas) {
          operator.scaleWorkerStatefulSetReplicas(operation.getExpectedWorkerReplicaNumber)
        }
      case _ =>
    }
  }

  protected def doScale(): Unit = {
    if (!isMasterActive) {
      return
    }
    if (!scaleUpEnabled && !scaleDownEnabled) {
      return
    }

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
}
