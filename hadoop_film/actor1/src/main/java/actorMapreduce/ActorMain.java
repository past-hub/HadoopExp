package actorMapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ActorMain {
    public static void main(String[] args) throws Exception{
        Job job=Job.getInstance(new Configuration());
        job.setJarByClass(ActorMain.class);
        //指定需要的jar包，路径为hdfs上的路径
        job.addFileToClassPath(new Path("/lib/fastjson-1.2.62.jar"));
        job.setMapperClass(ActorMapper.class);
        job.setMapOutputKeyClass(MovieInfo.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
