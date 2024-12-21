package org.apache.celeborn.service.deploy.worker.metrics

import java.util
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import com.codahale.metrics.{Clock, Counter, Gauge, Histogram, Meter, MetricAttribute, MetricFilter, MetricRegistry, ScheduledReporter, Snapshot, Timer}
import org.apache.celeborn.service.deploy.worker.Worker

import scala.collection.JavaConverters.asScalaSetConverter

class WorkerMetricReporter(
    registry: MetricRegistry,
    name: String,
    filter: MetricFilter,
    rateUnit: TimeUnit,
    durationUnit: TimeUnit,
    executor: ScheduledExecutorService,
    shutdownExecutorOnStop: Boolean,
    disabledMetricAttributes: util.Set[MetricAttribute],
    clock: Clock,
    worker: Worker) extends ScheduledReporter(
    registry,
    name,
    filter,
    rateUnit,
    durationUnit,
    executor,
    shutdownExecutorOnStop,
    disabledMetricAttributes) {
  val stats = worker.workerStatusManager.getWorkerStats

  override def report(
      gauges: util.SortedMap[String, Gauge[_]],
      counters: util.SortedMap[String, Counter],
      histograms: util.SortedMap[String, Histogram],
      meters: util.SortedMap[String, Meter],
      timers: util.SortedMap[String, Timer]): Unit = {
    val timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime)

    for (entry <- gauges.entrySet.asScala) {
      reportGauge(timestamp, entry.getKey, entry.getValue)
    }

    for (entry <- counters.entrySet.asScala) {
      reportCounter(timestamp, entry.getKey, entry.getValue)
    }

    for (entry <- histograms.entrySet.asScala) {
      reportHistogram(timestamp, entry.getKey, entry.getValue)
    }

    for (entry <- meters.entrySet.asScala) {
      reportMeter(timestamp, entry.getKey, entry.getValue)
    }

    for (entry <- timers.entrySet.asScala) {
      reportTimer(timestamp, entry.getKey, entry.getValue)
    }
  }

  def reportTimer(timestamp: Long, name: String, timer: Timer ): Unit = {
    val snapshot = timer.getSnapshot
    stats.put(name, snapshot.getMean.toString)
  }

  def reportMeter(timestamp: Long, name: String, meter: Meter): Unit = {
    stats.put(name, meter.getCount.toString)
  }

  def reportHistogram(timestamp: Long, name: String, histogram: Histogram): Unit = {
    stats.put(name, histogram.getSnapshot.getMedian.toString)
  }

  private def reportCounter(timestamp: Long, name: String, counter: Counter): Unit = {
    stats.put(name, counter.getCount.toString)
  }

  private def reportGauge(timestamp: Long, name: String, gauge: Gauge[_]): Unit = {
    stats.put(name, gauge.getValue.toString)
  }
}
