package com.taobao.csp.sentinel.dashboard.repository.zookeeper;

import com.alibaba.csp.sentinel.datasource.Converter;
import com.taobao.csp.sentinel.dashboard.datasource.entity.rule.DegradeRuleEntity;
import com.taobao.csp.sentinel.dashboard.rule.DynamicRuleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by kevin.chen on 2019/1/16 10:21.
 */
@Component("degradeRuleZookeeperProvider")
public class DegradeRuleZookeeperProvider implements DynamicRuleProvider<List<DegradeRuleEntity>> {

    private final Logger logger = LoggerFactory.getLogger(DegradeRuleZookeeperProvider.class);

    @Value("${zookeeper.remoteAddress}")
    private String remoteAddress;
    @Autowired
    private Converter<String, List<DegradeRuleEntity>> converter;

    @Override
    public List<DegradeRuleEntity> getRules(String appName) {
        CuratorFramework zkClient = null;
        try {
            zkClient = CuratorFrameworkFactory.newClient(remoteAddress,
                    new ExponentialBackoffRetry(1000, 3));
            zkClient.start();
            String path = ZookeeperConfigUtil.getPath(appName, ZookeeperConfigUtil.DEGRADE_RULES);
            byte[] bytes = zkClient.getData().forPath(path);
            String string = new String(bytes);
            return converter.convert(string);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("DegradeRuleZookeeperProvider getRules exception", e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return null;
    }
}
