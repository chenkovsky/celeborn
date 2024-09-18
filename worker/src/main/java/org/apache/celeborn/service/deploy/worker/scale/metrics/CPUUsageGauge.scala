package org.apache.celeborn.service.deploy.worker.scale.metrics

import com.codahale.metrics.Gauge

import java.lang.management.ManagementFactory
import javax.management.{MBeanServer, ObjectName}
import scala.util.control.NonFatal

class CPUUsageGauge extends Gauge[Long] {
  val mBean: MBeanServer = ManagementFactory.getPlatformMBeanServer
  val name = new ObjectName("java.lang", "type", "OperatingSystem")

  override def getValue: Long = {
    try {
      // return JVM process CPU time if the ProcessCpuTime method is available
      (mBean.getAttribute(name, "ProcessCpuLoad").asInstanceOf[Double] * 100).toLong
    } catch {
      case NonFatal(_) => 0L
    }
  }
}
