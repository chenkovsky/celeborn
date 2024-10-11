package org.apache.celeborn.service.deploy.worker.scale;

import org.apache.celeborn.common.meta.WorkerStats;
import org.apache.celeborn.service.deploy.worker.Worker;

public interface IWorkerStatsManager {
    void init(Worker worker);
    void stop();
    WorkerStats currentWorkerStats();
}
