package com.github.ntsiridis.tinkering.zookeeper.zkenv;

import com.github.ntsiridis.tinkering.zookeeper.IZookeeperNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Utility class to create a zookeeper
 * environment and test different scenarios
 */
public class ZkEnvironment {


    // Zookeeper address
    private final String zookeeperHost;
    // all the nodes that p
    private final ConcurrentMap<String, EnvNode> nodes = new ConcurrentHashMap<>();
    // thread pool for nodes
    private final ExecutorService executorService;
    private volatile boolean started = false;

    private Logger logger = LoggerFactory.getLogger(ZkEnvironment.class);

    public ZkEnvironment(String zkHost, IZookeeperNode... nodes) {
        this.zookeeperHost = zkHost;
        Arrays.stream(nodes).forEach(node -> this.nodes.putIfAbsent(node.getId(), new EnvNode(node, this.zookeeperHost)));
        this.executorService = Executors.newFixedThreadPool(this.nodes.size());
    }


    /**
     * Start all nodes
     *
     * @return
     */
    public ZkEnvironment start() throws InterruptedException {
        //add start messages to nodes
        this.nodes.values().forEach(node -> {
            node.addMessage(new ZkEnvNodeMessage(ZkEnvMessageType.START));
            executorService.submit(node);
        });
        this.started = true;

        return this;
    }

    /**
     * Returns true if the environment is started
     *
     * @return
     */
    public boolean isStarted() {
        return started;
    }



    /**
     * Shutdown the environment, and stop all nodes
     *
     * @return
     */
    public ZkEnvironment shutdown() {

        if (!isStarted()) {
            return this;
        }

        try {
            //send message to nodes to stop
            for (EnvNode node : this.nodes.values()) {
                node.addMessage(new ZkEnvNodeMessage(ZkEnvMessageType.STOP));
            }
            this.executorService.shutdown();
            if (!this.executorService.awaitTermination(1L, TimeUnit.SECONDS)) {
                this.executorService.shutdownNow();
            }
            this.started = false;
        } catch (InterruptedException e) {
            this.executorService.shutdownNow();
        }

        return this;
    }


    /**
     * Send a message to a specific node running in the environment
     *
     * @param nodeId , the id of the node
     * @param message, the message to send
     * @throws IllegalArgumentException
     */
    public void sendMessageToNode(String nodeId, ZkEnvNodeMessage message) throws IllegalArgumentException {

        checkStarted();

        //find node with the specified id
        if (nodeId == null || !this.nodes.containsKey(nodeId)) {
            throw new IllegalArgumentException("No node with id:" + nodeId + " is present");
        }

        EnvNode node = this.nodes.get(nodeId);
        node.addMessage(message);
    }

    /**
     * Return a slice with all the nodes
     * @return
     */
    public ZkEnvSlice allNodes() {

        Map<String, IZookeeperNode> nodes = new HashMap<>();
        this.nodes.values().forEach(n-> nodes.put(n.getNode().getId(), n.getNode()));
        return new ZkEnvSlice(nodes);
    }

    /**
     * Return only the active nodes
     * @return
     */
    public ZkEnvSlice activeNodes() {
        return new ZkEnvSlice(
                this.nodes.values().stream()
                        .filter( n-> n.getNode().isRunning())
                        .collect(Collectors.toMap(EnvNode::getId, EnvNode::getNode))
        );

    }

    /**
     * Get a slice with nodes of a specific type
     *
     * @param cz type of nodes
     * @param <T> subclass of IZookeeperNode
     * @return
     */
    public <T extends IZookeeperNode> ZkEnvSlice nodesOfType(final Class<T> cz) {
        return new ZkEnvSlice(
                this.nodes.values().stream()
                    .filter( n-> cz.isInstance(n.getNode()))
                    .collect(Collectors.toMap(EnvNode::getId, EnvNode::getNode))
        );
    }


    /**
     * Internal class used for creating the
     * runnable environment for each zookeeper
     * node
     */
    private static class EnvNode implements Runnable {

        // how long to wait for zookeeper connections
        private final int defaultWaitTime = 1500;
        private final IZookeeperNode node;
        private final String zookeeperURL;
        private final BlockingQueue<ZkEnvNodeMessage> incomingMessages = new LinkedBlockingDeque(10);
        private Logger logger = LoggerFactory.getLogger(ZkEnvironment.class);

        private EnvNode(IZookeeperNode node, String zookeeperConnectString) {
            this.node = node;
            this.zookeeperURL = zookeeperConnectString;
        }

        public String getId() {
            return this.node.getId();
        }

        public IZookeeperNode getNode() {
            return node;
        }


        public ZkEnvNodeMessage getNextMessage() throws InterruptedException {

            ZkEnvNodeMessage msg = this.incomingMessages.poll(100, TimeUnit.MILLISECONDS);
            if (msg == null) {
                return new ZkEnvNodeMessage(ZkEnvMessageType.SLEEP);
            }
            return msg;
        }

        public EnvNode addMessage(ZkEnvNodeMessage message)  {
            try {
                this.incomingMessages.put(message);
            } catch (InterruptedException e) {
                this.addMessage(
                        new ZkEnvNodeMessage(ZkEnvMessageType.ERROR)
                                .setDescription(e.getMessage())
                                .setData(e));
            }
            return this;
        }

        @Override
        public void run() {
            while (true) {
                //loop forever waiting  for the next message
                try {
                    ZkEnvNodeMessage msg = this.getNextMessage();
                    switch (msg.getType()) {
                        case UNDEFINED:
                            logger.error("Node:" + getNode().getId() + " received an undefined message.Message" + msg);
                            return;
                        case ERROR:
                            logger.error("Node:" + getNode().getId() + " Error:" + msg.getDescription());
                            //stop thread
                            return;
                        case SLEEP:
                            Thread.sleep(200);
                            break;
                        case START:
                            logger.info("Node:" + getNode().getId() + " starting...");
                            getNode().start(this.zookeeperURL, this.defaultWaitTime);
                            break;
                        case STOP:
                            //break loop, stop running
                            getNode().stop();
                            logger.info("Node:" + getNode().getId() + " is stopping...");
                            return;
                    }
                } catch (IOException | InterruptedException e) {
                    this.addMessage(
                            new ZkEnvNodeMessage(ZkEnvMessageType.ERROR)
                                    .setDescription(e.getMessage())
                                    .setData(e));
                }
            }
        }
    }



    /*
    Check that the environment is initialized
     */
    private void checkStarted() throws IllegalStateException {
        if (!isStarted()) {
            throw new IllegalStateException("Environment not started");
        }
    }



}
