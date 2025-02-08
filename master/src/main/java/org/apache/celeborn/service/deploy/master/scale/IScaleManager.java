package org.apache.celeborn.service.deploy.master.scale;

import org.apache.celeborn.server.common.service.config.ConfigService;
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager;

public interface IScaleManager {
    void init(ConfigService configService, AbstractMetaManager statusSystem);

    void run();

    void stop();

    long request(ScaleRequest request);
}
