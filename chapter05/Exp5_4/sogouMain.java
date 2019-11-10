package hadoop.ch05.v17124080229;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class sogouMain {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Job job = Job.getInstance();
        job.setJarByClass(sogouMain.class);

        job.setMapperClass(sogouMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(sogou.class);

        job.setPartitionerClass(sogouPartitioner.class);
        job.setNumReduceTasks(2);


        job.setReducerClass(sogouReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
