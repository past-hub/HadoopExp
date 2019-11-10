package hadoop.ch03;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.URI;
public class UploadHDFSFile {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.30.130:8020");
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");
        Path dfs = new Path("/17124080229");
        Path local = new Path("D:\\test4.txt");
        fs.copyFromLocalFile(false,true,local,dfs);
        fs.close();
    }
}