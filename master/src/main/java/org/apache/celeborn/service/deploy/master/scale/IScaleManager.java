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

import org.apache.celeborn.server.common.service.config.ConfigService;
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager;

/**
 * Interface defining the contract for Celeborn's worker scaling functionality.
 * Implementations of this interface manage the scaling operations of worker nodes
 * in different deployment environments (e.g., Kubernetes, standalone).
 */
public interface IScaleManager {
    /**
     * Initializes the scale manager with necessary services.
     *
     * @param configService Service providing configuration settings
     * @param statusSystem System managing cluster metadata and worker status
     */
    void init(ConfigService configService, AbstractMetaManager statusSystem);

    /**
     * Starts a background thread that periodically polls the cluster state
     * and performs auto-scaling operations based on the current resource utilization.
     * The polling thread continuously monitors the system and triggers scale up/down
     * operations when needed.
     */
    void run();

    /**
     * Stops the background polling thread and terminates the auto-scaling operations.
     * This method ensures a graceful shutdown of the polling mechanism.
     */
    void stop();
}
