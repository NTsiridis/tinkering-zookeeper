package com.github.ntsiridis.tinkering.zookeeper.zkenv;

import com.github.ntsiridis.tinkering.zookeeper.IZookeeperNode;

import java.util.*;

/**
 * A "slice" of the environment
 * is a selection of the environment nodes
 */
public class ZkEnvSlice {

    private Map<String, IZookeeperNode> nodesSlice = new HashMap<>();

    public ZkEnvSlice(Map<String, IZookeeperNode> nodesSlice) {
        this.nodesSlice = nodesSlice;
    }

    public Map<String, IZookeeperNode> getNodesSlice() {
        return nodesSlice;
    }

    /**
     *
     * @return The slice size
     */
    public int size() {
        return this.nodesSlice.size();
    }


    /**
     * Get the contained nodes as a list
     * @return
     */
    public List<IZookeeperNode> getNodes() { return new ArrayList<>(this.nodesSlice.values());}

    /**
     * Perform an assertion on the current environment state
     *
     * @param assertion the assertion to test
     * @return true / false , result of the assertion
     */
    public boolean assertThat(ZkEnvAssertion assertion) {
        return assertion.assertThat(this.getNodesSlice());
    }

}
