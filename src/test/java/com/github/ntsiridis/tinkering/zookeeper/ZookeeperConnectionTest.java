package com.github.ntsiridis.tinkering.zookeeper;

import com.github.ntsiridis.tinkering.zookeeper.master_worker.AdminMasterWorker;
import com.github.ntsiridis.tinkering.zookeeper.master_worker.BaseMasterNode;
import com.github.ntsiridis.tinkering.zookeeper.master_worker.BaseWorkerNode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ZookeeperConnectionTest {

    private Logger logger = LoggerFactory.getLogger(ZookeeperConnectionTest.class);


    @Container
    public static GenericContainer zk = new GenericContainer(DockerImageName.parse("zookeeper"))
            .withExposedPorts(2181);


    @Test
    public void testConnection() throws IOException {
        String address = zk.getHost() + ":" + zk.getMappedPort(2181);
        ZooKeeper underTest = new ZooKeeper(address, 1500, new TestWatcher());
        assertNotNull(underTest);
    }

    @Test
    public void testCreateMasterAndWorker() throws IOException, InterruptedException, KeeperException {
        final String address = zk.getHost() + ":" + zk.getMappedPort(2181);

        ExecutorService service = Executors.newFixedThreadPool(3);
        CountDownLatch latch = new CountDownLatch(3);

        BaseMasterNode masterNode = new BaseMasterNode();
        service.submit(()->{
            assertTrue(masterNode.isMaster() == false);
            try {
                masterNode.start(address,1500);
                latch.countDown();
            } catch (IOException e) {

            }
        });

        AdminMasterWorker adminNode = new AdminMasterWorker();
        service.submit(()->{
            try {
                adminNode.start(address);
                String currentState = adminNode.listState();
                System.out.println("One master. Current State:" + currentState);
            } catch (IOException | InterruptedException | KeeperException e) {

            }
            latch.countDown();

        });

        service.submit(()->{
            BaseWorkerNode workerNode1 = new BaseWorkerNode();
            try {
                workerNode1.start(address, 1500);
                String currentState = adminNode.listState();
                System.out.println("One master - One worker. Current State:" + currentState);
            } catch (IOException | InterruptedException | KeeperException e) {
            }
            latch.countDown();
        });

        latch.await();
    }


    /*

     */
    private class TestWatcher implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {
            logger.info("Got event:" + watchedEvent);
        }
    }


}
