/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.taobao.csp.sentinel.dashboard.service;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.function.Tuple2;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.taobao.csp.sentinel.dashboard.client.SentinelApiClient;
import com.taobao.csp.sentinel.dashboard.domain.cluster.ClusterAppAssignResultVO;
import com.taobao.csp.sentinel.dashboard.domain.cluster.ClusterGroupEntity;
import com.taobao.csp.sentinel.dashboard.domain.cluster.config.ClusterClientConfig;
import com.taobao.csp.sentinel.dashboard.domain.cluster.config.ServerFlowConfig;
import com.taobao.csp.sentinel.dashboard.domain.cluster.config.ServerTransportConfig;
import com.taobao.csp.sentinel.dashboard.domain.cluster.request.ClusterAppAssignMap;
import com.taobao.csp.sentinel.dashboard.repository.zookeeper.ClusterConfigZookeeperPublisher;
import com.taobao.csp.sentinel.dashboard.util.MachineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Eric Zhao
 * @since 1.4.1
 */
@Service
public class ClusterAssignServiceImpl implements ClusterAssignService {

    private final Logger LOGGER = LoggerFactory.getLogger(ClusterAssignServiceImpl.class);

    @Autowired
    private SentinelApiClient sentinelApiClient;
    @Autowired
    private ClusterConfigService clusterConfigService;
    @Autowired
    private ClusterConfigZookeeperPublisher clusterConfigZookeeperPublisher;

    @Override
    public ClusterAppAssignResultVO unbindClusterServer(String app, String machineId) {
        AssertUtil.assertNotBlank(app, "app cannot be blank");
        AssertUtil.assertNotBlank(machineId, "machineId cannot be blank");
        Set<String> failedSet = new HashSet<>();

        try {
            ClusterGroupEntity entity = clusterConfigService.getClusterUniversalStateForAppMachine(app, machineId)
                .get(10, TimeUnit.SECONDS);
            Set<String> toModifySet = new HashSet<>();
            toModifySet.add(machineId);
            if (entity.getClientSet() != null) {
                toModifySet.addAll(entity.getClientSet());
            }
            // Modify mode to NOT-STARTED for all chosen token servers and associated token clients.
            toModifySet.parallelStream()
                .map(MachineUtils::parseCommandIpAndPort)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(e -> {
                    CompletableFuture<Void> f = modifyMode(app, e.r1, e.r2, ClusterStateManager.CLUSTER_NOT_STARTED);
                    return Tuple2.of(e.r1 + '@' + e.r2, f);
                })
                .forEach(f -> handleFutureSync(f, failedSet));
        } catch (Exception ex) {
            Throwable e = ex instanceof ExecutionException ? ex.getCause() : ex;
            LOGGER.error("Failed to unbind machine <{}>", machineId, e);
            failedSet.add(machineId);
        }
        return new ClusterAppAssignResultVO()
            .setFailedClientSet(failedSet)
            .setFailedServerSet(new HashSet<>());
    }

    @Override
    public ClusterAppAssignResultVO unbindClusterServers(String app, Set<String> machineIdSet) {
        AssertUtil.assertNotBlank(app, "app cannot be blank");
        AssertUtil.isTrue(machineIdSet != null && !machineIdSet.isEmpty(), "machineIdSet cannot be empty");
        ClusterAppAssignResultVO result = new ClusterAppAssignResultVO()
            .setFailedClientSet(new HashSet<>())
            .setFailedServerSet(new HashSet<>());
        for (String machineId : machineIdSet) {
            ClusterAppAssignResultVO resultVO = unbindClusterServer(app, machineId);
            result.getFailedClientSet().addAll(resultVO.getFailedClientSet());
            result.getFailedServerSet().addAll(resultVO.getFailedServerSet());
        }
        return result;
    }

    @Override
    public ClusterAppAssignResultVO applyAssignToApp(String app, List<ClusterAppAssignMap> clusterMap,
                                                     Set<String> remainingSet) {
        AssertUtil.assertNotBlank(app, "app cannot be blank");
        AssertUtil.notNull(clusterMap, "clusterMap cannot be null");
        Set<String> failedServerSet = new HashSet<>();
        Set<String> failedClientSet = new HashSet<>();

        // Assign server and apply config.
        clusterMap.stream()
            .filter(Objects::nonNull)
            .filter(ClusterAppAssignMap::getBelongToApp)
            .map(e -> {
                String ip = e.getIp();
                int commandPort = parsePort(e);
                CompletableFuture<Void> f = modifyMode(app, ip, commandPort, ClusterStateManager.CLUSTER_SERVER)
                    .thenCompose(v -> applyServerConfigChange(app, ip, commandPort, e));
                return Tuple2.of(e.getMachineId(), f);
            })
            .forEach(t -> handleFutureSync(t, failedServerSet));

        // Assign client of servers and apply config.
        clusterMap.parallelStream()
            .filter(Objects::nonNull)
            .forEach(e -> applyAllClientConfigChange(app, e, failedClientSet));

        // Unbind remaining (unassigned) machines.
        applyAllRemainingMachineSet(app, remainingSet, failedClientSet);

        // save into zookeeper
        List<ClusterGroupEntity> clusterGroupEntities = Lists.newArrayList();
        for (ClusterAppAssignMap assignMap : clusterMap){
            ClusterGroupEntity clusterGroupEntity = new ClusterGroupEntity();
            clusterGroupEntity.setMachineId(assignMap.getMachineId());
            clusterGroupEntity.setClientSet(assignMap.getClientSet());
            clusterGroupEntity.setIp(assignMap.getIp());
            clusterGroupEntity.setPort(assignMap.getPort());
            clusterGroupEntities.add(clusterGroupEntity);
        }
        clusterConfigZookeeperPublisher.modifyClusterServerConfig(app, null, JSON.toJSONString(clusterGroupEntities));



        return new ClusterAppAssignResultVO()
            .setFailedClientSet(failedClientSet)
            .setFailedServerSet(failedServerSet);
    }

    private void applyAllRemainingMachineSet(String app, Set<String> remainingSet, Set<String> failedSet) {
        if (remainingSet == null || remainingSet.isEmpty()) {
            return;
        }
        remainingSet.parallelStream()
            .filter(Objects::nonNull)
            .map(MachineUtils::parseCommandIpAndPort)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(ipPort -> {
                String ip = ipPort.r1;
                int commandPort = ipPort.r2;
                CompletableFuture<Void> f = modifyMode(app, ip, commandPort, ClusterStateManager.CLUSTER_NOT_STARTED);
                return Tuple2.of(ip + '@' + commandPort, f);
            })
            .forEach(t -> handleFutureSync(t, failedSet));
    }

    private void applyAllClientConfigChange(String app, ClusterAppAssignMap assignMap,
                                            Set<String> failedSet) {
        Set<String> clientSet = assignMap.getClientSet();
        if (clientSet == null || clientSet.isEmpty()) {
            return;
        }
        final String serverIp = assignMap.getIp();
        final int serverPort = assignMap.getPort();
        clientSet.stream()
            .map(MachineUtils::parseCommandIpAndPort)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(ipPort -> {
                CompletableFuture<Void> f = sentinelApiClient
                    .modifyClusterMode(app, ipPort.r1, ipPort.r2, ClusterStateManager.CLUSTER_CLIENT)
                    .thenCompose(v -> sentinelApiClient.modifyClusterClientConfig(app, ipPort.r1, ipPort.r2,
                        new ClusterClientConfig().setRequestTimeout(20)
                            .setServerHost(serverIp)
                            .setServerPort(serverPort)
                    ));
                return Tuple2.of(ipPort.r1 + '@' + ipPort.r2, f);
            })
            .forEach(t -> handleFutureSync(t, failedSet));
    }

    private void handleFutureSync(Tuple2<String, CompletableFuture<Void>> t, Set<String> failedSet) {
        try {
            t.r2.get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            if (ex instanceof ExecutionException) {
                LOGGER.error("Request for <{}> failed", t.r1, ex.getCause());
            } else {
                LOGGER.error("Request for <{}> failed", t.r1, ex);
            }
            failedSet.add(t.r1);
        }
    }

    private CompletableFuture<Void> applyServerConfigChange(String app, String ip, int commandPort,
                                                            ClusterAppAssignMap assignMap) {
        ServerTransportConfig transportConfig = new ServerTransportConfig()
            .setPort(assignMap.getPort())
            .setIdleSeconds(600);
        return sentinelApiClient.modifyClusterServerTransportConfig(app, ip, commandPort, transportConfig)
            .thenCompose(v -> applyServerFlowConfigChange(app, ip, commandPort, assignMap))
            .thenCompose(v -> applyServerNamespaceSetConfig(app, ip, commandPort, assignMap));
    }

    private CompletableFuture<Void> applyServerFlowConfigChange(String app, String ip, int commandPort,
                                                                ClusterAppAssignMap assignMap) {
        Double maxAllowedQps = assignMap.getMaxAllowedQps();
        if (maxAllowedQps == null || maxAllowedQps <= 0 || maxAllowedQps > 20_0000) {
            return CompletableFuture.completedFuture(null);
        }
        return sentinelApiClient.modifyClusterServerFlowConfig(app, ip, commandPort,
            new ServerFlowConfig().setMaxAllowedQps(maxAllowedQps));
    }

    private CompletableFuture<Void> applyServerNamespaceSetConfig(String app, String ip, int commandPort,
                                                                  ClusterAppAssignMap assignMap) {
        Set<String> namespaceSet = assignMap.getNamespaceSet();
        if (namespaceSet == null || namespaceSet.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return sentinelApiClient.modifyClusterServerNamespaceSet(app, ip, commandPort, namespaceSet);
    }

    private CompletableFuture<Void> modifyMode(String app, String ip, int port, int mode) {
        return sentinelApiClient.modifyClusterMode(app, ip, port, mode);
    }

    private int parsePort(ClusterAppAssignMap assignMap) {
        return MachineUtils.parseCommandPort(assignMap.getMachineId())
            .orElse(ServerTransportConfig.DEFAULT_PORT);
    }
}
