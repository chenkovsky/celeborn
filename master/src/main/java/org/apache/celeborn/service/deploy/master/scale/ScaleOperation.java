package org.apache.celeborn.service.deploy.master.scale;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

public class ScaleOperation {
    private long lastScaleOperationEndTime = 0L;
    private int expectedWorkerReplicaNumber = -1;
    private List<ScalingWorker> needRecommissionWorkers = new ArrayList<>();
    private List<ScalingWorker> needDecommissionWorkers = new ArrayList<>();
    private ScaleType scaleType = ScaleType.STABILIZATION;

    public ScaleOperation() {
    }

    public ScaleOperation(
            long lastScaleOperationEndTime,
            int expectedWorkerReplicaNumber,
            List<ScalingWorker> needRecommissionWorkers,
            List<ScalingWorker> needDecommissionWorkers,
            ScaleType scaleType) {
        this.lastScaleOperationEndTime = lastScaleOperationEndTime;
        this.expectedWorkerReplicaNumber = expectedWorkerReplicaNumber;
        this.needRecommissionWorkers = needRecommissionWorkers;
        this.needDecommissionWorkers = needDecommissionWorkers;
        this.scaleType = scaleType;
    }

    public long getLastScaleOperationEndTime() {
        return lastScaleOperationEndTime;
    }

    public void setLastScaleOperationEndTime(long lastScaleOperationEndTime) {
        this.lastScaleOperationEndTime = lastScaleOperationEndTime;
    }

    public int getExpectedWorkerReplicaNumber() {
        return expectedWorkerReplicaNumber;
    }

    public void setExpectedWorkerReplicaNumber(int expectedWorkerReplicaNumber) {
        this.expectedWorkerReplicaNumber = expectedWorkerReplicaNumber;
    }

    public List<ScalingWorker> getNeedRecommissionWorkers() {
        return needRecommissionWorkers;
    }

    public void setNeedRecommissionWorkers(List<ScalingWorker> needRecommissionWorkers) {
        this.needRecommissionWorkers = needRecommissionWorkers;
    }

    public List<ScalingWorker> getNeedDecommissionWorkers() {
        return needDecommissionWorkers;
    }

    public void setNeedDecommissionWorkers(List<ScalingWorker> needDecommissionWorkers) {
        this.needDecommissionWorkers = needDecommissionWorkers;
    }

    public ScaleType getScaleType() {
        return scaleType;
    }

    public void setScaleType(ScaleType scaleType) {
        this.scaleType = scaleType;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("lastScaleOperationEndTime", getLastScaleOperationEndTime())
                .append("needRecommissionWorkers", getNeedRecommissionWorkers())
                .append("needDecommissionWorkers", getNeedDecommissionWorkers())
                .append("scaleType", getScaleType())
                .toString();
    }
}
