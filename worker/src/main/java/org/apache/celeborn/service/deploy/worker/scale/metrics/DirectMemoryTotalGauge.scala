package org.apache.celeborn.service.deploy.worker.scale.metrics

import com.codahale.metrics.Gauge
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class DirectMemoryTotalGauge(memoryManager: MemoryManager) extends Gauge[Long] {

  override def getValue: Long = memoryManager.getMemoryUsage
}