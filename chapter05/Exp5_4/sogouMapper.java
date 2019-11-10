package hadoop.ch05.v17124080229;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class sogouMapper extends Mapper< LongWritable, Text, NullWritable, sogou> {
    @Override
    protected void map(LongWritable k1, Text v1,
                       Context context)
            throws IOException, InterruptedException {
        String data = v1.toString();
        String[] words = data.split(",");
        sogou dt = new sogou();
        //00:00:06 9441949621471399   [韩国饰品] 1 1    www.uczx.com/
        dt.setDate(words[0]);
        dt.setUserid(words[1]);
        dt.setWord(words[2]);
        dt.setRank(Integer.parseInt(words[3]));
        dt.setOrder(Integer.parseInt(words[4]));
        dt.setUrl(words[5]);
        NullWritable k2 = NullWritable.get();
        context.write(k2, dt);
    }
}
