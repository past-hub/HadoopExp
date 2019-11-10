package hadoop.ch03;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;


import java.io.InputStream;
import java.net.URI;
public class ReadHDFSFile {
    public static void main(String[] args)throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.30.130:8020");
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");
        Path dfs = new Path("/17124080229/test5.txt");

        InputStream is = fs.open(dfs);
        IOUtils.copyBytes(is,System.out,1024,false);
        is.close();

    }
}
