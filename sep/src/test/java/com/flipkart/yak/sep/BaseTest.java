package com.flipkart.yak.sep;

import info.batey.kafka.unit.KafkaUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class BaseTest {
  protected final Logger LOG = LoggerFactory.getLogger(BaseTest.class);

  private final static int NUM_SLAVES_BASE = 4;
  private final int zkPort = 9002;
  private final int kafkaPort = 9003;
  private KafkaUnit kafkaUnitServer;
  private final String TABLE = "test";
  private final ClusterId CLUSTER_ID1 = new ClusterId("b6a97322-e990-47f6-a4d9-b5851c99a921");
  private final ClusterId CLUSTER_ID2 = new ClusterId("b6a97322-e990-47f6-a4d9-b5851c99a922");
  private final String KAFKA_REPLICATION_PEER = "testKafkaReplicationEndpoint";
  private final String CLUSTER_REPLICATION_PEER = "testClusterReplicationEndpoint";
  private final TableName table = TableName.valueOf(TABLE);
  private HBaseTestingUtility TEST_UTIL;
  private HBaseTestingUtility TEST_UTIL2;

  protected Configuration conf = HBaseConfiguration.create();
  protected Configuration conf2 = HBaseConfiguration.create();
  protected final byte[] cf1 = "cf".getBytes();
  protected final byte[] cf2 = "cc".getBytes();
  protected final byte[] cf3 = "cd".getBytes();
  protected final byte[] cf4 = "ce".getBytes();
  protected boolean shouldSetupSecondCluster = false;

  protected void startKafkaServer() throws Exception {
    kafkaUnitServer = new KafkaUnit(zkPort, kafkaPort);
    kafkaUnitServer.startup();
    kafkaUnitServer.createTopic("yak_export");
    kafkaUnitServer.createTopic("yak_export_2");
  }

  protected void doPut(byte[] columnFamily, byte[] rowKey, byte[] qualifier, byte[] column)
      throws IOException {
    Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    try (Table t = connection.getTable(table)) {
      Put put = new Put(rowKey);
      put.addColumn(columnFamily, qualifier, column);
      t.put(put);
    }
  }

  protected void doDelete(byte[] columnFamily, byte[] rowKey, byte[] qualifier)
          throws IOException {
    Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    try (Table t = connection.getTable(table)) {
      Delete delete = new Delete(rowKey);
      Put put = new Put(rowKey);
      delete.addColumn(columnFamily, qualifier);
      t.delete(delete);
    }
  }

  protected void doPut2(byte[] columnFamily, byte[] rowKey, byte[] qualifier, byte[] column)
      throws IOException {
    Connection connection = ConnectionFactory.createConnection(TEST_UTIL2.getConfiguration());
    try (Table t = connection.getTable(table)) {
      Put put = new Put(rowKey);
      put.addColumn(columnFamily, qualifier, column);
      t.put(put);
    }
  }

  private void setupMiniCluster() throws Exception {
    TEST_UTIL = new HBaseTestingUtility(conf);
    MiniDFSCluster cluster = TEST_UTIL.startMiniDFSCluster(3);
    DistributedFileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    Path rd = TEST_UTIL.getDefaultRootDirPath();

    FSUtils
        .setClusterId(fs, rd, CLUSTER_ID1, conf.getInt("hbase.server.thread.wakefrequency", 10000));

    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE);
    TEST_UTIL.getDFSCluster().waitClusterUp();
    TEST_UTIL.getConfiguration()
        .setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_SLAVES_BASE);
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(60000);

    ReplicationPeerConfigBuilder replicationPeerConfigBuilder = ReplicationPeerConfig.newBuilder()
        .setClusterKey(ZKConfig.getZooKeeperClusterKey(TEST_UTIL.getConfiguration()))
        .setReplicationEndpointImpl(getReplicationEndpoint()).setSerial(true)
        .setReplicateAllUserTables(true);
    TEST_UTIL.getAdmin()
        .addReplicationPeer(KAFKA_REPLICATION_PEER, replicationPeerConfigBuilder.build());
    startKafkaServer();
  }

  private void setupSecondCluster() throws Exception {
    TEST_UTIL2 = new HBaseTestingUtility(conf2);
    MiniDFSCluster cluster = TEST_UTIL2.startMiniDFSCluster(3);
    DistributedFileSystem fs = TEST_UTIL2.getDFSCluster().getFileSystem();
    Path rd = TEST_UTIL2.getDefaultRootDirPath();

    FSUtils.setClusterId(fs, rd, CLUSTER_ID2,
        conf2.getInt("hbase.server.thread.wakefrequency", 10000));
    TEST_UTIL2.startMiniZKCluster();

    TEST_UTIL2.startMiniCluster(NUM_SLAVES_BASE);
    TEST_UTIL2.getDFSCluster().waitClusterUp();
    TEST_UTIL2.getConfiguration()
        .setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_SLAVES_BASE);
    TEST_UTIL2.getMiniHBaseCluster().waitForActiveAndReadyMaster(60000);

    ReplicationPeerConfigBuilder replicationPeerConfigBuilder = ReplicationPeerConfig.newBuilder()
        .setClusterKey(ZKConfig.getZooKeeperClusterKey(TEST_UTIL.getConfiguration()))
        .setSerial(true).setReplicateAllUserTables(true);
    TEST_UTIL2.getAdmin()
        .addReplicationPeer(CLUSTER_REPLICATION_PEER, replicationPeerConfigBuilder.build());
  }

  private void createTable() throws Exception {
    HTableDescriptor tableDesc = new HTableDescriptor(table);
    HColumnDescriptor fam1 = new HColumnDescriptor(cf1);
    fam1.setMaxVersions(3);
    fam1.setScope(1);
    tableDesc.addFamily(fam1);

    HColumnDescriptor fam2 = new HColumnDescriptor(cf2);
    fam2.setMaxVersions(3);
    fam2.setScope(1);
    tableDesc.addFamily(fam2);

    HColumnDescriptor fam3 = new HColumnDescriptor(cf3);
    fam3.setMaxVersions(3);
    fam3.setScope(1);
    tableDesc.addFamily(fam3);

    HColumnDescriptor fam4 = new HColumnDescriptor(cf4);
    fam4.setMaxVersions(3);
    fam4.setScope(1);
    tableDesc.addFamily(fam4);

    TEST_UTIL.getAdmin().createTable(tableDesc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    TEST_UTIL.waitTableAvailable(table);
    TEST_UTIL.getAdmin().flush(table);

    if (shouldSetupSecondCluster) {
      TEST_UTIL2.getAdmin().createTable(tableDesc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      TEST_UTIL2.waitTableAvailable(table);
      TEST_UTIL2.getAdmin().flush(table);
    }
  }

  @Before public void setUp() throws Exception {
    setupMiniCluster();
    if (shouldSetupSecondCluster) {
      setupSecondCluster();
    }
    createTable();
  }

  private void tearDownCluster(HBaseTestingUtility utility, String replicationPeerId)
      throws Exception {
    utility.getAdmin().disableTable(table);
    utility.getAdmin().deleteTable(table);

    if (utility.getAdmin() != null) {
      utility.getAdmin().removeReplicationPeer(replicationPeerId);
      utility.getAdmin().close();
    }

    if (utility != null) {
      utility.shutdownMiniCluster();
    }
  }

  @After public void tearDown() throws Exception {
    tearDownCluster(TEST_UTIL, KAFKA_REPLICATION_PEER);

    if (shouldSetupSecondCluster) {
      tearDownCluster(TEST_UTIL2, CLUSTER_REPLICATION_PEER);
    }

    if (kafkaUnitServer != null) {
      kafkaUnitServer.shutdown();
    }
  }

  String getReplicationEndpoint() {
    return KafkaReplicationEndPoint.class.getName();
  }
}
