package com.github.ntsiridis.tinkering.zookeeper.master_worker;

import com.github.ntsiridis.tinkering.zookeeper.AbstractNode;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


/**
 * Base implementation of a master node
 */
public class BaseMasterNode extends AbstractNode {

    private Logger logger = LoggerFactory.getLogger(BaseMasterNode.class);
    private boolean master;
    private Set<String> workerIds = new HashSet<>();

    // call back for become master request
    // acknowledgment
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    //check if the call failed
                    //or if the call succeeded,and
                    //we didn't learn about it
                    checkIfWeAreMasterNode();
                    return;
                case NODEEXISTS:
                    //there is already a master
                    masterExists();
                    master = false;
                case OK:
                    //we are now the master node
                    master = true;
                    //bootstrap the system
                    bootstrap();
                    return;
                default:
                    master = false;
            }
            logger.info("Node:" + getId() + " , run for leader " + (master? "succeeded":"failed"));
        }
    };



    // static call back to check if we are master
    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    //are we the master ?
                    master = new String(data).equals(getId());
                    if (master) {
                        bootstrap();
                    }
                    return;
                case NONODE:
                    //zookeeper respond with the
                    //absence of a master node.
                    //Attempt to become master
                    attemptToBecomeMasterNode();
                    return;
                case CONNECTIONLOSS:
                    //lost our connection
                    //before getting the answer
                    //back. Check again
                    checkIfWeAreMasterNode();
                    return;
            }
            logger.info("Master node " + getId() + " check if master returns" + master);
        }
    };


    /*
    Callback for creating parent nodes during bootstrapping
     */
    AsyncCallback.StringCallback createParentNodeCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    //try to create the parent node again
                    createParentNode(path, (byte[]) ctx);
                    break;
                case OK:
                    // we created the node
                    logger.info("Bootstrapping. Create node:" + name);
                    break;
                case NODEEXISTS:
                    // someone else created the node
                    // .No worries
                    break;
                default:
                    logger.error("Node " + getId() + " reported error " + KeeperException.Code.get(rc) + " at path " + name);
            }

        }
    };

    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getAvailableWorkerIds();
                    break;
                case OK:
                    workerIds.addAll(children);
                    break;
                default:
                    logger.error("Getting workers failed", KeeperException.create(KeeperException.Code.get(rc), path));

            }
        }
    };

    // Watcher for another master
    private Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                attemptToBecomeMasterNode();
            }
        }
    };

    private Watcher workersChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                getAvailableWorkerIds();
            }
        }
    };



    @Override
    public void notify(WatchedEvent event) {
        logger.debug("Node:" + getId() + " , event:" + event);
    }


    /**
     * Returns true if this node is master
     * @return
     */
    public boolean isMaster() {
        return master;
    }

    /**
     * Start master node
     * @param hostConnectionString
     * @param timeoutInMills
     * @throws IOException
     */
    @Override
    public void start(String hostConnectionString, int timeoutInMills) throws IOException {
        super.start(hostConnectionString, timeoutInMills);
        attemptToBecomeMasterNode();
    }



    /**
     *
     * @return the available worker ids
     */
    public Set<String> getAvailableWorkerIds() {
        return this.workerIds;
    }

    /**
     * Refresh worker lists
     */
    public void getWorkers() {
        getConnection().getChildren(
                MasterWorkerPaths.WORKERS_PATH,
                workersChangeWatcher,
                workersGetChildrenCallback,
                null
        ) ;
    }



    /**
     *
     * @param command
     * @return
     * @throws KeeperException
     */
    public String queueCommand(String command) throws KeeperException {
        while (true) {
            try {
                //create the node as task-1, task-2 etc.
                String name = getConnection().create(
                        MasterWorkerPaths.TASKS_PATH + "/task-",
                        command.getBytes(StandardCharsets.UTF_8),
                        OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (InterruptedException e) {
                //retry the creation of the node
            }
        }
    }

    /*
    Create all the necessary parent nodes for
    the master worker model to work
     */
    protected void bootstrap() {
        createParentNode(MasterWorkerPaths.WORKERS_PATH, new byte[0]);
        createParentNode(MasterWorkerPaths.TASKS_PATH, new byte[0]);
        createParentNode(MasterWorkerPaths.ASSIGN_PATH, new byte[0]);
        createParentNode(MasterWorkerPaths.STATUS_PATH, new byte[0]);
    }

    /*
    Utility to create a root node during bootstrapping
     */
    protected void createParentNode(String path, byte[] data) {
        getConnection().create(
                path,
                data,
                OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentNodeCallback,
                data);
    }

    /*
    Check if the current node is a master node
     */
    protected void checkIfWeAreMasterNode() {
        //check if there is a master node with
        //our id
        getConnection().getData(
                MasterWorkerPaths.MASTER_PATH,
                false,
                masterCheckCallback,
                null);
    }

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    break;
                case NONODE:
                    attemptToBecomeMasterNode();
                    break;
                default:
                    checkIfWeAreMasterNode();
                    break;

            }
        }
    };

    /*
    Notify me if a master node changes

     */
    private void masterExists() {
        getConnection().exists(
                MasterWorkerPaths.MASTER_PATH,
                masterExistsWatcher,
                masterExistsCallback,
                null
                );
    }

    /*
     * Try to become the master node
     */
    protected void attemptToBecomeMasterNode()  {

        //async call zookeeper to create a master node
        //and wait for the call back to see what happened
        //The master node in zookeeper will be created
        //as an ephemeral node, allowing other nodes
        //to become master if the current node, goes away
        getConnection().create(
                MasterWorkerPaths.MASTER_PATH,
                getId().getBytes(StandardCharsets.UTF_8),
                OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null
        );
    }




}
