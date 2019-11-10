package hadoop.ch05.v17124080229;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
public class sogouPartitioner extends Partitioner<NullWritable, sogou>{
    @Override
    public int getPartition(NullWritable k2, sogou v2, int numPartitioner) {


        if(v2.getRank() == 2 && v2.getOrder() == 1) {

            return 1%numPartitioner;
        }else {
            return 2%numPartitioner;
        }
    }
}