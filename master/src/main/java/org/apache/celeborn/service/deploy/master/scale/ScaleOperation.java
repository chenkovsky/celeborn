/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.master.scale;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

public class ScaleOperation {
    private long lastScaleUpEndTime = 0L;
    private long lastScaleDownEndTime = 0L;
    private long currentScaleStartTime = 0L;
    private int expectedWorkerReplicaNumber = -1;
    private List<ScalingWorker> needRecommissionWorkers = new ArrayList<>();
    private List<ScalingWorker> needDecommissionWorkers = new ArrayList<>();
    private ScaleType scaleType = ScaleType.STABILIZATION;

    public ScaleOperation() {
    }

    public ScaleOperation(
            long lastScaleUpEndTime,
            long lastScaleDownEndTime,
            long currentScaleStartTime,
            int expectedWorkerReplicaNumber,
            List<ScalingWorker> needRecommissionWorkers,
            List<ScalingWorker> needDecommissionWorkers,
            ScaleType scaleType) {
        this.lastScaleUpEndTime = lastScaleUpEndTime;
        this.lastScaleDownEndTime = lastScaleDownEndTime;
        this.currentScaleStartTime = currentScaleStartTime;
        this.expectedWorkerReplicaNumber = expectedWorkerReplicaNumber;
        this.needRecommissionWorkers = needRecommissionWorkers;
        this.needDecommissionWorkers = needDecommissionWorkers;
        this.scaleType = scaleType;
    }

    public long getLastScaleUpEndTime() {
        return lastScaleUpEndTime;
    }

    public void setLastScaleUpEndTime(long lastScaleUpEndTime) {
        this.lastScaleUpEndTime = lastScaleUpEndTime;
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

    public long getLastScaleDownEndTime() {
        return lastScaleDownEndTime;
    }

    public void setLastScaleDownEndTime(long time) {
        this.lastScaleDownEndTime = time;
    }

    public long getCurrentScaleStartTime() {
        return currentScaleStartTime;
    }

    public void setCurrentScaleStartTime(long currentScaleStartTime) {
        this.currentScaleStartTime = currentScaleStartTime;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("lastScaleUpEndTime", getLastScaleUpEndTime())
                .append("lastScaleDownEndTime", getLastScaleDownEndTime())
                .append("currentScaleStartTime", getCurrentScaleStartTime())
                .append("expectedWorkerReplicaNumber", getExpectedWorkerReplicaNumber())
                .append("needRecommissionWorkers", getNeedRecommissionWorkers())
                .append("needDecommissionWorkers", getNeedDecommissionWorkers())
                .append("scaleType", getScaleType())
                .toString();
    }
}
