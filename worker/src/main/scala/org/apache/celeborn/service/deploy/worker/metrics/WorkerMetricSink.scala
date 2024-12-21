package org.apache.celeborn.service.deploy.worker.metrics

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.service.deploy.worker.Worker


class WorkerMetricSink(conf: CelebornConf) extends IWorkerMetricSink {

  private var workerMetricReporter: WorkerMetricReporter = _

  private var worker: Worker = _

  override def stop(): Unit = {
    workerMetricReporter.stop()
  }

  override def start(): Unit = {
    workerMetricReporter.start()
  }

  override def report(): Unit = {
    workerMetricReporter.report()
  }

  override def init(worker: Worker): Unit = {
    this.worker = worker
  }
}
