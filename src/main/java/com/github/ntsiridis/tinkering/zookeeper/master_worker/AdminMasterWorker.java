package com.github.ntsiridis.tinkering.zookeeper.master_worker;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * An administration utility to show the contents of zookeeper
 */
public class AdminMasterWorker implements Watcher {

    private ZooKeeper zk;
    private Logger logger = LoggerFactory.getLogger(AdminMasterWorker.class);

    public void start(String zookeeperHost) throws IOException {
        this.zk = new ZooKeeper(zookeeperHost, 15000, this);
    }

    public String listState() throws InterruptedException, KeeperException {
        StringBuffer str = new StringBuffer();
        try {
            Stat stat = new Stat();
            byte masterData[] = this.zk.getData(MasterWorkerPaths.MASTER_PATH, false, stat);
            Date startDate = new Date(stat.getCtime());
            str.append("Master:" + new String(masterData) + " since:" + startDate + "\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NoNodeException) {
                str.append("No master node\n");
            } else {
                this.logger.error("Error while getting state:" + e.getMessage());
            }
        }

        str.append("Workers:\n");
        for(String w:zk.getChildren(MasterWorkerPaths.WORKERS_PATH,false)) {
            byte data[] = zk.getData(MasterWorkerPaths.WORKERS_PATH+"/" + w,false, null);
            String state = new String(data);
            str.append("\t" + w + ":" + state + "\n");
        }

        str.append("Tasks:\n");
        for(String t:zk.getChildren(MasterWorkerPaths.TASKS_PATH,false)) {
            byte data[] = zk.getData(MasterWorkerPaths.TASKS_PATH+"/" + t,false, null);
            String cmd = new String(data);
            str.append("\t" + t + ":" + cmd);
        }

        return str.toString();

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        this.logger.info("Admin event:" + watchedEvent);
    }
}
