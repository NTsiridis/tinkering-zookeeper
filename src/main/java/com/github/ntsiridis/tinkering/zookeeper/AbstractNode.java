package com.github.ntsiridis.tinkering.zookeeper;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/**
 * An abstract implementation of a master
 * zookeeper node
 */
public abstract class AbstractNode
        implements Watcher,IZookeeperNode {

    private ZooKeeper zk;
    private final String id = UUID.randomUUID().toString();
    protected boolean running = false;

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void start(String hostConnectionString, int timeoutInMills) throws IOException {
        this.zk = new ZooKeeper(hostConnectionString, timeoutInMills, this );
        this.running = true;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public void stop() throws InterruptedException {
        this.zk.close();
        this.running = false;
    }

    /**
     * Returns the connection to the zookeeper, if any
     * @return
     */
    public ZooKeeper getConnection() {
        return zk;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        notify(watchedEvent);
    }

    /**
     * Specific implementation of events coming to this node
     *
     * @param event, to event to be processed
     */

    public abstract void notify(WatchedEvent event);


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IZookeeperNode)) return false;
        IZookeeperNode that = (IZookeeperNode) o;
        return getId().equals(that.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }
}
