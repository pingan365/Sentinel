package com.taobao.csp.sentinel.dashboard.repository.zookeeper;

import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.fastjson.JSON;
import com.taobao.csp.sentinel.dashboard.datasource.entity.rule.FlowRuleEntity;
import com.taobao.csp.sentinel.dashboard.rule.DynamicRulePublisher;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by kevin.chen on 2019/1/16 10:47.
 */
@Component("flowRuleZookeeperPublisher")
public class FlowRuleZookeeperPublisher implements DynamicRulePublisher<List<FlowRuleEntity>> {

    private final Logger logger = LoggerFactory.getLogger(FlowRuleZookeeperPublisher.class);

    @Value("${zookeeper.remoteAddress}")
    private String remoteAddress;
    @Autowired
    private Converter<List<FlowRuleEntity>, String> converter;

    @Override
    public void publish(String app, List<FlowRuleEntity> rules) throws Exception {
        CuratorFramework zkClient = null;
        try {
            zkClient = CuratorFrameworkFactory.newClient(remoteAddress,
                    new ExponentialBackoffRetry(1000, 3));
            zkClient.start();
            String path = ZookeeperConfigUtil.getPath(app, ZookeeperConfigUtil.FLOW_RULES);
            Stat stat = zkClient.checkExists().forPath(path);
            if (stat == null) {
                zkClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, null);
            }
            zkClient.setData().forPath(path, converter.convert(rules).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("FlowRuleZookeeperPublisher publish exception, rules:{}", JSON.toJSON(rules), e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
