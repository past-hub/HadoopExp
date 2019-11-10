package hadoop.ch05.v17124080229;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class sogouReducer extends Reducer<NullWritable,sogou,NullWritable,Text> {

    @Override
    protected void reduce(NullWritable k3, Iterable<sogou> v3,
                          Context context) throws IOException, InterruptedException {
        String line=null;
        for (sogou v1 : v3) {
            line = v1.toString();
            context.write(k3, new Text(line));
        }
    }
}
