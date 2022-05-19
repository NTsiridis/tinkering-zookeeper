package com.github.ntsiridis.tinkering.zookeeper;

import com.github.ntsiridis.tinkering.zookeeper.master_worker.BaseMasterNode;
import com.github.ntsiridis.tinkering.zookeeper.master_worker.BaseWorkerNode;
import com.github.ntsiridis.tinkering.zookeeper.zkenv.ZkEnvMessageType;
import com.github.ntsiridis.tinkering.zookeeper.zkenv.ZkEnvNodeMessage;
import com.github.ntsiridis.tinkering.zookeeper.zkenv.ZkEnvironment;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class MasterWorkerTest {



    @Container
    public static GenericContainer<?> zk = new GenericContainer<>(DockerImageName.parse("zookeeper"))
            .withExposedPorts(2181);


    @Test
    public void masterNodeTest() throws InterruptedException {
        String address = zk.getHost() + ":" + zk.getMappedPort(2181);
        BaseMasterNode master = new BaseMasterNode();
        BaseWorkerNode worker = new BaseWorkerNode();
        ZkEnvironment env = new ZkEnvironment(address,
                master,
                worker);

        env.start();
        await().atMost(6,SECONDS).until(env::isStarted);

        assertTrue(env.allNodes().assertThat((currentNodes) -> currentNodes.size() == 2));
        assertTrue(env.nodesOfType(BaseMasterNode.class).assertThat((currentNodes) -> currentNodes.size() == 1));

        env.shutdown();
    }

    @Test
    public void doubleMasterNodeTest() throws InterruptedException {
        String address = zk.getHost() + ":" + zk.getMappedPort(2181);
        BaseMasterNode master1 = new BaseMasterNode();
        BaseMasterNode master2 = new BaseMasterNode();
        ZkEnvironment env = new ZkEnvironment(address,
                master1,
                master2);

        env.start();
        await().atMost(6,SECONDS).until(env::isStarted);

        assertTrue(env.allNodes().assertThat((currentNodes) -> currentNodes.size() == 2));
        assertTrue(env.nodesOfType(BaseMasterNode.class).assertThat((currentNodes) -> currentNodes.size() == 2));
        assertTrue(env.activeNodes().size() == 2);

        //find current master
        Optional<IZookeeperNode> currentMaster = env.nodesOfType(BaseMasterNode.class).getNodesSlice().values().stream()
                .filter(node->((BaseMasterNode)node).isMaster()).findFirst();

        assertTrue(currentMaster.isPresent());
        String currentMasterId = currentMaster.get().getId();

        //stop node
        env.sendMessageToNode(currentMaster.get().getId(), new ZkEnvNodeMessage(ZkEnvMessageType.STOP));
        await().atMost(2, SECONDS).until(()->env.activeNodes().size() == 1 );
        BaseMasterNode newMaster = (BaseMasterNode) env.activeNodes().getNodes().get(0);
        assertTrue( newMaster.isMaster());
        assertTrue( !newMaster.getId().equals(currentMasterId));

        env.shutdown();
    }

    @Test
    public void registerWorkersTest()  throws InterruptedException {
        String address = zk.getHost() + ":" + zk.getMappedPort(2181);
        BaseMasterNode master = new BaseMasterNode();
        BaseWorkerNode worker1 = new BaseWorkerNode();
        BaseWorkerNode worker2 = new BaseWorkerNode();
        ZkEnvironment env = new ZkEnvironment(address,
                master,
                worker1,
                worker2);

        env.start();
        await().atMost(6,SECONDS).until(env::isStarted);
        assertTrue(env.activeNodes().size() == 3);

        //find current master
        Optional<IZookeeperNode> currentMaster = env.nodesOfType(BaseMasterNode.class).getNodesSlice().values().stream()
                .filter(node->((BaseMasterNode)node).isMaster()).findFirst();
        BaseMasterNode masterNode = (BaseMasterNode) currentMaster.get();

        await().atMost(6, SECONDS).until(()->{
            masterNode.getWorkers();
            return masterNode.getAvailableWorkerIds().size() != 0;
        });
        assertTrue(masterNode.getAvailableWorkerIds().size()  == 2);
        env.shutdown();
    }

}
