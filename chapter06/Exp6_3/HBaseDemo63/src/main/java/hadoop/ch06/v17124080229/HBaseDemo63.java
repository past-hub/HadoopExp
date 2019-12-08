package hadoop.ch06.v17124080229;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.List;


public class HBaseDemo63 {
    public static void main(String[] args) throws Exception {
        CreateTable();
        insert();
        scan_all();
        get();
        scan();


    }

    private static void CreateTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1");
        conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed", "true");
        Connection connect = ConnectionFactory.createConnection(conf);
        Admin admin = connect.getAdmin();
        TableName tn = TableName.valueOf("emp17124080229");
        String[] family = new String[]{"member_id", "address", "info"};
        HTableDescriptor desc = new HTableDescriptor(tn);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tn)) {
            System.out.println("------------创建表-----------------------");
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("------------创建表-----------------------");
            System.out.println("create table Success!");
        }
    }
    private static void insert() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1");
        conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed", "true");
        Connection connect = ConnectionFactory.createConnection(conf);
//        TableName tableName = TableName.valueOf("tb");
        Table table = connect.getTable(TableName.valueOf("emp17124080229"));
        Put put = new Put(Bytes.toBytes("Rain"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("member_name"), Bytes.toBytes("林作镇"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("member_id"), Bytes.toBytes("17124080229"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("21"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1998-07-08"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("ShanWei"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("China"));
        List<Put> list = new ArrayList();
        list.add(put);
        table.put(list);
        System.out.println("------------插入表-----------------------");
        System.out.println("insert success");
    }
    private static void scan() throws  Exception{
        String rowKey = "Rain";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1");
        conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed", "true");
        Connection connect = ConnectionFactory.createConnection(conf);
        Table table = connect.getTable(TableName.valueOf("emp17124080229"));
        Get get = new Get(rowKey.getBytes());
//        get.addFamily(Bytes.toBytes("info"));
        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"));
        Result result = table.get(get);
        System.out.println("------------扫描age列-----------------------");
        System.out.println("行键\t列族\t列名\t值");
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +  //Rowkey
                            Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" + //CF
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +//qualifier
                            Bytes.toString(CellUtil.cloneValue(cell))//value
            );
        }
    }
    private static void get() throws Exception {
        String rowKey = "Rain";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1");
        conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed", "true");
        Connection connect = ConnectionFactory.createConnection(conf);
        Table table = connect.getTable(TableName.valueOf("emp17124080229"));
        Get get = new Get(rowKey.getBytes());
        get.addFamily(Bytes.toBytes("info"));
        Result result = table.get(get);
        System.out.println("-------------------获取info列族---------------------");
        System.out.println("行键\t列族\t列名\t值");
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +  //Rowkey
                            Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" + //CF
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +//qualifier
                            Bytes.toString(CellUtil.cloneValue(cell))//value
            );
        }

    }
    private static void scan_all() throws  Exception{
        String rowKey = "Rain";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1");
        conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
        conf.set("hbase.cluster.distributed", "true");
        Connection connect = ConnectionFactory.createConnection(conf);
        Table table = connect.getTable(TableName.valueOf("emp17124080229"));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        System.out.println("------------扫描整个表-----------------------");
        System.out.println("行键\t列族\t列名\t值");
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +  //Rowkey
                            Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" + //CF
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +//qualifier
                            Bytes.toString(CellUtil.cloneValue(cell))//value
            );
        }
    }

        }

