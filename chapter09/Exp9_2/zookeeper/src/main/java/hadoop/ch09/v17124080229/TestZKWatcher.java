package hadoop.ch09.v17124080229;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class TestZKWatcher {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 创建一个与服务器的连接
        ZooKeeper zk = new ZooKeeper(ZookeeperConstant.URL, 1000 * 1, new Watcher() {
            // 监控所有被触发的事件
            public void process(WatchedEvent event) {
                System.out.println("已经触发了" + event.getType() + "事件！"+ event);
            }
        });


        // 创建一个目录节点
        zk.create("/stu1", "stu17124080229".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("stu1节点的数据为"+new String(zk.getData("/stu1", false, null)));
        System.out.println("-----------------------------------------------------");
        // 创建一个子目录节点
        zk.create("/stu1/stu17124080229", "stu17124080229的数据为stu1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("stu1子节点数据已发生变化，最新数据为为"+new String(zk.getData("/stu1/stu17124080229", false, null)));
        System.out.println("-----------------------------------------------------");
        // 取出子目录节点列表
        System.out.println("stu1节点下的子节点列表"+zk.getChildren("/stu1", true));
        System.out.println("-----------------------------------------------------");
        // 修改子目录节点数据
        zk.setData("/stu1/stu17124080229", "stu17124080229的数据为stu2".getBytes(), -1);
        System.out.println("目录节点状态：[" + zk.exists("/stu1", true) + "]");
        System.out.println("stu1子节点数据已发生变化，最新数据为为"+new String(zk.getData("/stu1/stu17124080229", false, null)));
        System.out.println("-----------------------------------------------------");
        // 创建另外一个子目录节点
        zk.create("/stu1/stu17124080229v2", "stu17124080229v2的数据为stu1v2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(new String(zk.getData("/stu1/stu17124080229v2", true, null)));
        // 取出子目录节点列表
        System.out.println("stu1节点下的子节点列表"+zk.getChildren("/stu1", true));
        System.out.println("-----------------------------------------------------");

        // 删除子目录节点
        zk.delete("/stu1/stu17124080229", -1);
        zk.delete("/stu1/stu17124080229v2", -1);

        // 删除父目录节点
        zk.delete("/stu1", -1);
//         关闭连接
        zk.close();
    }

}