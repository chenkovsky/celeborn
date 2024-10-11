package org.apache.celeborn.common.scale;

import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.WorkerEventType;

public interface IScaleManager {
    WorkerEventType handleHeartbeatFromWorker(WorkerInfo worker);
}
