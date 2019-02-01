package com.taobao.csp.sentinel.dashboard.repository.zookeeper;

import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.fastjson.JSON;
import com.taobao.csp.sentinel.dashboard.datasource.entity.rule.DegradeRuleEntity;
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
 * Created by kevin.chen on 2019/1/16 10:50.
 */
@Component("degradeRuleZookeeperPublisher")
public class DegradeRuleZookeeperPublisher implements DynamicRulePublisher<List<DegradeRuleEntity>> {

    private final Logger logger = LoggerFactory.getLogger(DegradeRuleZookeeperProvider.class);

    @Value("${zookeeper.remoteAddress}")
    private String remoteAddress;
    @Autowired
    private Converter<List<DegradeRuleEntity>, String> converter;

    @Override
    public void publish(String app, List<DegradeRuleEntity> rules) {
        CuratorFramework zkClient = null;
        try {
            zkClient = CuratorFrameworkFactory.newClient(remoteAddress,
                    new ExponentialBackoffRetry(1000, 3));
            zkClient.start();
            String path = ZookeeperConfigUtil.getPath(app, ZookeeperConfigUtil.DEGRADE_RULES);
            Stat stat = zkClient.checkExists().forPath(path);
            if (stat == null) {
                zkClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, null);
            }
            zkClient.setData().forPath(path, converter.convert(rules).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("DegradeRuleZookeeperPublisher publish exception, rules:{}", JSON.toJSON(rules), e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}