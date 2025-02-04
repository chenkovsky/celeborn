package org.apache.celeborn.service.deploy.master.scale;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class ScaleRequest {
    private final String userIdentifier;
    private final String tagsExpr;
    private final int num;

    public ScaleRequest(String userIdentifier, String tagsExpr, int num) {
        this.userIdentifier = userIdentifier;
        this.tagsExpr = tagsExpr;
        this.num = num;
    }

    public String getUserIdentifier() {
        return userIdentifier;
    }

    public String getTagsExpr() {
        return tagsExpr;
    }

    public int getNum() {
        return num;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("userIdentifier", getUserIdentifier())
                .append("tagsExpr", getTagsExpr())
                .append("num", getNum())
                .toString();
    }
}
