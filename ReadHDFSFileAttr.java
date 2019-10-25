package hadoop.ch03;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.URI;


public class ReadHDFSFileAttr {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.30.130:8020");
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");
        Path dfs = new Path("/17124080229/test5.txt");
        String st = "17124080229";
        byte[] b=st.getBytes();
        fs.setXAttr(dfs, "user.id", b);
        String res = new String(fs.getXAttr(dfs, "user.id"));
        System.out.println(res);
    }
}

