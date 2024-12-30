package org.apache.celeborn.service.deploy.worker.metrics

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.service.deploy.worker.Worker

import java.util.concurrent.TimeUnit


class WorkerMetricSink(conf: CelebornConf) extends IWorkerMetricSink {

  private var reporter: WorkerMetricReporter = _

  private var worker: Worker = _

  private val pollPeriod = conf.workerHeartbeatTimeout / 4

  private val pollUnit: TimeUnit = TimeUnit.SECONDS

  override def stop(): Unit = {
    reporter.stop()
  }

  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  override def report(): Unit = {
    reporter.report()
  }

  override def init(worker: Worker): Unit = {
    this.worker = worker
  }
}
