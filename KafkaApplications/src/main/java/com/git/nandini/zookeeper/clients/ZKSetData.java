package com.git.nandini.zookeeper.clients;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

public class ZKSetData {
    private static ZooKeeper zk;
    private static ZooKeeperConnection conn;

    // Method to update the data in a znode. Similar to getData but without watcher.
    public static void update(String path, byte[] data) throws
            KeeperException,InterruptedException {
        zk.setData(path, data, zk.exists(path,true).getVersion());
    }

    public static void main(String[] args) throws InterruptedException,KeeperException {
        String path= "/MyFirstZnode";
        byte[] data = "Success".getBytes(); //Assign data which is to be updated.

        try {
            conn = new ZooKeeperConnection();
            zk = conn.connect("localhost");
            update(path, data); // Update znode data to the specified path
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

/*
cd /path/to/zookeeper
bin/zkCli.sh
>>> get /MyFirstZnode
 */