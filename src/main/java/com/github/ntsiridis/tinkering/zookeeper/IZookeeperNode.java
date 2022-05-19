package com.github.ntsiridis.tinkering.zookeeper;

import java.io.IOException;
import java.util.UUID;

/**
 * Basic interface that all nodes
 * implement. A node is an abstract concept
 * that denotes a software service that
 * plays a role in a distributed environment
 * that is coordinated by a zookeeper ensemble.
 */
public interface IZookeeperNode {



    /**
     * Return the id of the node
     * @return
     */
    String getId();

    /**
     * Connect to a zookeeper ensemble
     * @param hostConnectionString , connection string
     * @param timeoutInMills , maximum time to wait for a connection (in milliseconds)
     * @throws IOException , when cannot connect to zookeeper
     */
    void start(final String hostConnectionString, final int timeoutInMills) throws IOException;


    /**
     * Disconnect from zookeeper
     *
     * @throws InterruptedException
     */
    void stop() throws InterruptedException;

    /**
     *
     *
     * @returnR true if the node is in a running state
     */
    boolean isRunning();


}
