package org.apache.celeborn.service.deploy.worker.scale

import com.codahale.metrics.Gauge

import scala.collection.JavaConverters._
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{TimeWindow, WorkerStats}
import org.apache.celeborn.common.scale.ScaleMetrics
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.service.deploy.worker.scale.metrics.{CPUUsageGauge, DirectMemoryTotalGauge, DirectMemoryUsageGauge}

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.collection.mutable


private[celeborn] class WorkerStatsManager (conf: CelebornConf) extends IWorkerStatsManager with Logging {
  private val slideWindowSize = conf.scaleSlidingWindowSize
  private val checkInterval = conf.scaleCheckInterval

  private val enabled = conf.scaleUpEnabled || conf.scaleDownEnabled

  private[this] var scheduler: ScheduledExecutorService = _

  private val metrics: mutable.Map[String, (Gauge[Long], TimeWindow)] = mutable.Map()

  def register(name: String, gauge: Gauge[Long]): Unit = {
    metrics.put(name, (gauge, new TimeWindow(slideWindowSize, 1)))
  }

  protected def run(): Unit = {
    metrics.foreach {case(k, (gauge, window)) => window.update(gauge.getValue) }
  }

  protected def initMetrics(worker: Worker): Unit = {
    register(ScaleMetrics.CPU_LOAD_USAGE, new CPUUsageGauge())
    register(ScaleMetrics.DIRECT_MEMORY_TOTAL, new DirectMemoryTotalGauge(worker.memoryManager))
    register(ScaleMetrics.DIRECT_MEMORY_USAGE, new DirectMemoryUsageGauge(worker.memoryManager))
  }

  override def init(worker: Worker): Unit = {
    if (!enabled) {
      return
    }
    initMetrics(worker)
    scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-jvm-scale-scheduler")
    scheduler.scheduleWithFixedDelay(
      new Runnable() {
        override def run(): Unit = {
          WorkerStatsManager.this.run()
        }
      },
      0,
      checkInterval,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    if (!enabled) {
      return
    }
    scheduler.shutdown()
  }

  override def currentWorkerStats: WorkerStats = if (enabled) {
    new WorkerStats(metrics.map{case (k, (_, v)) => k -> v.getAverage().toString}.asJava)
  } else {
    WorkerStats.defaultWorkerStats()
  }
}
