package org.apache.celeborn.service.deploy.master.scale;

import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.commons.lang3.builder.ToStringBuilder;


public class ScalingWorker {
    private String name; // pod name
    private WorkerInfo worker;

    public ScalingWorker(String name, WorkerInfo worker) {
        this.name = name;
        this.worker = worker;
    }

    public String getName() {
        return name;
    }

    public WorkerInfo getWorker() {
        return worker;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", getName())
                .append("worker", getWorker())
                .toString();
    }
}
