package com.github.ntsiridis.tinkering.zookeeper.master_worker;

import com.github.ntsiridis.tinkering.zookeeper.AbstractNode;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.awaitility.core.ConditionEvaluationListener;
import org.awaitility.core.ConditionEvaluationLogger;
import org.awaitility.core.EvaluatedCondition;
import org.awaitility.core.TimeoutEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;

public class BaseWorkerNode extends AbstractNode {

    private Logger logger = LoggerFactory.getLogger(BaseWorkerNode.class);
    private String status;
    private final String name = "worker-" + getId();

    /*
    Call back for the registration of the client
     */
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    //attempt again
                    register();
                    return;
                case OK:
                    logger.info("Node " + getId() + " is registered as worker");
                    return;
                case NODEEXISTS:
                    //already registered
                    break;
                case NONODE:
                    logger.info("NO NODE for:" + getId()  + " ..reconnect");
                    //possibly there isn't a parent node yet
                    with().conditionEvaluationListener(new ConditionEvaluationListener() {
                                @Override
                                public void conditionEvaluated(EvaluatedCondition evaluatedCondition) {
                                    //do nothing
                                }

                                @Override
                                public void onTimeout(TimeoutEvent timeoutEvent) {
                                    logger.info("Worker " + getId() +" was unable to register (no root parent node). Wait a little...");
                                }
                            })
                            .await().ignoreExceptions()
                            .atMost(500, MILLISECONDS)
                            .until(
                                    ()-> getConnection().exists(MasterWorkerPaths.WORKERS_PATH, false) != null
                            );

                    //attempt again to register
                    register();
                default:
                    logger.error("Node " + name + " got error while trying to register. Error:" + KeeperException.Code.get(rc));

            }
        }
    };

    /*
    Callback for status update
     */
    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    };

    /**
     * Start a worker node
     *
     * @param hostConnectionString, connection info for zookeeper hosts
     * @param timeoutInMills, how long to wait for a connection
     * @throws IOException
     */
    @Override
    public void start(String hostConnectionString, int timeoutInMills) throws IOException {
        super.start(hostConnectionString, timeoutInMills);
        register();
    }


    @Override
    public void notify(WatchedEvent event) {
        logger.debug("Worker Node " + getId() + "Got event " + event);

    }

    synchronized public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    /*
    Utility method to update the status of the worker.
    Synchronized because it is called from the async callback
    and also as a normal call, from setStatus
     */
    synchronized private void updateStatus(String status) {
        if (status.equals(this.status)) {
            getConnection().setData(
                    MasterWorkerPaths.WORKERS_PATH  + name,
                    status.getBytes(StandardCharsets.UTF_8),
                    -1,
                    statusUpdateCallback,
                    status);
        }
    }

    /**
     * Register the worker and wait for job assignment
     */
    private void register() {
        //create an ephemeral node with status "Idle"
        //under the worker path in order to declare
        //the existence of the worker
        getConnection().create(
                MasterWorkerPaths.WORKERS_PATH + "/" + name,
                "Idle".getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null
        );
    }

}
