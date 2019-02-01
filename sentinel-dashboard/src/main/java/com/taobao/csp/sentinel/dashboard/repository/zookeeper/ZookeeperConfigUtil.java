package com.taobao.csp.sentinel.dashboard.repository.zookeeper;

import org.apache.commons.lang.StringUtils;

/**
 * Created by kevin.chen on 2019/1/16 10:16.
 */
public class ZookeeperConfigUtil {
    public static final String GROUP_ID = "/sentinel";
    public static final String FLOW_RULES = "/rules/flow-rules";
    public static final String DEGRADE_RULES="/rules/degrade-rules";

    public static final String CONFIG_DATAID = "/cluster-client-config";
    public static final String CLUSTER_MAP_DATAID = "/cluster-map";

    public static String getPath(String groupId, String dataId) {
        String path = ZookeeperConfigUtil.GROUP_ID;
        if (groupId.startsWith("/")) {
            path += groupId;
        } else {
            path += "/" + groupId;
        }
        if (StringUtils.isNotBlank(dataId)){
            if (dataId.startsWith("/")) {
                path += dataId;
            } else {
                path += "/" + dataId;
            }
        }
        return path;
    }

    private ZookeeperConfigUtil() {}
}
