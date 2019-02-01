package com.taobao.csp.sentinel.dashboard.repository.zookeeper;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by kevin.chen on 2019/1/16 11:30.
 */
@Component
public class ClusterConfigZookeeperPublisher {

    @Value("${zookeeper.remoteAddress}")
    private String remoteAddress;

    public void modifyClusterClientConfig(String app,String ip ,String data){
        if (StringUtils.isBlank(data)){
            return ;
        }
        doPublish(app, ZookeeperConfigUtil.CONFIG_DATAID, data);
    }

    public void modifyClusterServerConfig(String app, String ip, String data){
        if (StringUtils.isBlank(data)){
            return ;
        }
        doPublish(app, ZookeeperConfigUtil.CLUSTER_MAP_DATAID, data);
    }

    private void doPublish(String app, String dataId, String data) {
        CuratorFramework zkClient = null;
        try {
            zkClient = CuratorFrameworkFactory.newClient(remoteAddress, new ExponentialBackoffRetry(1000, 3));
            zkClient.start();
            //String path = ZookeeperConfigUtil.getPath(app,ip);
            String path = ZookeeperConfigUtil.getPath(app, dataId);
            Stat stat = zkClient.checkExists().forPath(path);
            if (stat == null) {
                zkClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, null);
            }
            zkClient.setData().forPath(path,data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
