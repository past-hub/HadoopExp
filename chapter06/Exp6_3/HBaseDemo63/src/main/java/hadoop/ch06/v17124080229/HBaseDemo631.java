package hadoop.ch06.v17124080229;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class HBaseDemo631 {
    //初始化
    private static Connection connection = null;
    private static Admin admin = null;
    //静态代码块@toolHuangxiaohong
    static {

        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","node1");
            configuration.set("hbase.rootdir", "hdfs://node1:8020/hbase");
            configuration.set("hbase.cluster.distributed", "true");
            //创建到 HBase 的连接
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isTableExist(String tableName) throws IOException {
        //获取配置信息
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum","node0");
//        configuration.set("hbase.rootdir", "hdfs://node0:8020/hbase");
//        configuration.set("hbase.cluster.distributed", "true");
//        获取管理员对象
//        Connection connection = ConnectionFactory.createConnection(configuration);
//        Admin admin = connection.getAdmin();
        //HBaseAdmin admin = new HBaseAdmin(configuration);
        //判断表名是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //关闭连接
//        admin.close();
        //返回结果
        return exists;
    }
    //构造close方法
    public static void close()  {
        if(admin != null);{
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(connection!=null);
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //创建表
    public static void createTable(String tableName,String... cfs) throws IOException {
        if (cfs.length<=0){
            System.out.println("请设置列族信息");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已存在");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);
    }
    //插入数据
    public static void insert(String rowKey, String tableName,
                              String[] column1, String[] value1, String[] column2, String[] value2,
                              String[] column3, String[] value3) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < columnFamilies.length; i++) {
            String f = columnFamilies[i].getNameAsString();
            if (f.equals("member_id")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (f.equals("address")) {
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
            if (f.equals("info")) {
                for (int j = 0; j < column3.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column3[j]), Bytes.toBytes(value3[j]));
                }
            }
        }
        table.put(put);
        table.close();
    }

    //获取表
    public static void get(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //Get get1 = new Get(Bytes.toBytes(rowKey));
        System.out.println("-----------------toolwanghaihong@查看"+cf+"列------------------");
        get.addFamily(Bytes.toBytes(cf));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行键："+Bytes.toString(CellUtil.cloneRow(cell))+", 列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
                    ", 列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    ", 值："+Bytes.toString(CellUtil.cloneValue(cell)));

        }
//        //System.out.println("查看"+cn+"列");
//        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
//        Result result = table.get(get);
//        for (Cell cell : result.rawCells()) {
//            System.out.println(" 行键："+Bytes.toString(CellUtil.cloneRow(cell))+", 列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
//                    ", 列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
//                    ", 值："+Bytes.toString(CellUtil.cloneValue(cell)));
//
//        }

    }
    //扫描表
    public static void scan(String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(rowKey));
        //Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));
        //Scan scan = new Scan();//无参构造，扫描全表咯
        scan.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println( " 行键："+Bytes.toString(CellUtil.cloneRow(cell))+", 列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
                        ", 列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        ", 值："+Bytes.toString(CellUtil.cloneValue(cell)));


            }

        }

        table.close();

    }
    //测试代码，主方法
    public static void main(String[] args) throws IOException {
//        System.out.println(isTableExist("stu"));
//        createTable( "stu","info1","info2");
//        System.out.println(isTableExist("stu"));
//        insert("stu","1001","info1","name","Wanghaihong");
//        get("stu","1001","info1","name");
        System.out.println("----------------------------------------------------------");
        System.out.println("班级：数学17-2");
        System.out.println("学号：17124080229");
        System.out.println("Hadoop实验6.3正在为你呈现...");
        System.out.println("----------------------------------------------------------");
        System.out.println("创建emp17124080229表,如果已经创建则会打印已创建");
        createTable("emp17124080234","member_id","address","info");
        System.out.println("----------------------------------------------------------");
        System.out.println("查看表是否存在");
        System.out.println(isTableExist("emp17124080229"));

        String [] column1 = {"id"};String [] column2 = {"city","country"};
        String [] column3 ={"age","birthday","industry"};
        String [] value1 = {"234"};String [] value2 = {"Meizhou","China"} ;
        String [] value3 = {"21","1998-06-18","student"};
        insert("","emp17124080229",column1,value1,column2,value2,column3,value3);

        get("emp17124080229","","info","city");

        System.out.println("-------------@扫描info:birthday列---------------");
        scan("emp17124080229","","info","birthday");
        System.out.println("-----------------@这是底线---------------------");
        close();
    }
}
