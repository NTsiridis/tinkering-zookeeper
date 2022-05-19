package com.github.ntsiridis.tinkering.zookeeper.zkenv;

import com.github.ntsiridis.tinkering.zookeeper.IZookeeperNode;

import java.util.Map;

/**
 * An interface for assertion on the environment
 */
public interface ZkEnvAssertion {

    /**
     * An assertion on the environment
     *
     * @param currentNodes, all the current nodes are passed as arguments to the assertion
     * @return true / false depending on the outcome of the assertion
     */
    boolean assertThat(Map<String, IZookeeperNode> currentNodes);
}
