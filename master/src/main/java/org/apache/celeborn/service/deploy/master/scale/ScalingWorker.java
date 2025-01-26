package org.apache.celeborn.service.deploy.master.scale;

import org.apache.commons.lang3.builder.ToStringBuilder;


public class ScalingWorker {
    private String name; // pod name
    private String uniqueId; // worker unique id

    public ScalingWorker(String name, String uniqueId) {
        this.name = name;
        this.uniqueId = uniqueId;
    }

    public boolean hasUniqueId() {
        return uniqueId != null;
    }

    public String getName() {
        return name;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", getName())
                .append("uniqueId", uniqueId)
                .toString();
    }
}
