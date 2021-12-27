package com.wugk.zktest.server;

import com.google.common.primitives.Longs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class CuratorServer {
    protected static final Logger logger = LoggerFactory.getLogger(CuratorServer.class);
    CuratorFramework curatorFramework;
    @Value("${zookeeper_ip}")
    String zkip;

    @Value("${zookeeper_port}")
    String zkPort;

    String role = "slave";

    public void init() {
        curatorFramework = CuratorFrameworkFactory.builder().
                connectString(zkip + ":" + zkPort).
                sessionTimeoutMs(5000).
                retryPolicy(new ExponentialBackoffRetry(1000, 3)).
                connectionTimeoutMs(1000).build();
        curatorFramework.start();
        AddWatch(curatorFramework);
    }

    private void AddWatch(CuratorFramework curatorFramework) {
        String root = "/root";
        String masterNodePath = root + "/master";
        String slaveNodePath = root + "/slave";

        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, root, true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    String childPath = event.getData().getPath();
                    logger.info("child remove " + childPath);
                    if (masterNodePath.equals(childPath)) {
                        switchMaster(curatorFramework, masterNodePath, slaveNodePath);
                    }
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CONNECTION_LOST)) {
                    logger.info("connection lost, became slave");
                    role = "slave";
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED)) {
                    logger.info("connection connected...");
                    if (!becameMaster(curatorFramework, masterNodePath)) {
                        becameSlave(curatorFramework, slaveNodePath);
                    }
                } else {
                    logger.info("path changed " + event.getData().getPath());
                }
            }




        });

//        TreeCache treeCache = new TreeCache(curatorFramework, "/");
//        treeCache.getListenable().addListener(new TreeCacheListener() {
//            @Override
//            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
//                logger.info(treeCacheEvent.toString());
//            }
//        });
//        try {
//            treeCache.start();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }




    public String create(String path, String value) throws Exception {
        curatorFramework.create().
                creatingParentContainersIfNeeded().
                withMode(CreateMode.PERSISTENT).
                inBackground().
                forPath(path, value.getBytes());
        return path;
    }

    private void switchMaster(CuratorFramework curatorFramework, String masterNodePath, String slaveNodePath) {
        if (becameMaster(curatorFramework, masterNodePath)) {
            try {
                curatorFramework.delete().forPath(slaveNodePath);
            } catch (Exception e) {
                logger.info("failed to delete slave node");
            }
        }
    }

    private boolean becameMaster(CuratorFramework curatorFramework, String masterNodePath) {
        try {
            curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).
                    forPath(masterNodePath, Longs.toByteArray(System.currentTimeMillis()));
            logger.info("succeed to became master");
            role = "master";
            return true;
        } catch (Exception e) {
            logger.error("failed to became master", e);
            return true;
        }
    }

    private boolean becameSlave(CuratorFramework curatorFramework, String slaveNodePath) {
        try {
            curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).
                    forPath(slaveNodePath, Longs.toByteArray(System.currentTimeMillis()));
            logger.info("succeed to became slave");
            role = "slave";
            return true;
        } catch (Exception e) {
            logger.error("failed to became slave", e);
            return true;
        }
    }

}
