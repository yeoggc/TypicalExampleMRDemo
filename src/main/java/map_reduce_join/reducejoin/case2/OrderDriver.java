package map_reduce_join.reducejoin.case2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 与case1 的区别是 由于只有一个reduce，
 * 所以不需要自定义partitioner,来保证相同pid的orderbean对象到同一个reduce中
 */
@SuppressWarnings("DuplicatedCode")
public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"input/map_reduce_join", "output/map_reduce_join/case2"};


        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);
        Path output = new Path(args[1]);
        fs.delete(output, true);

        Job job = Job.getInstance();
        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);


        job.setNumReduceTasks(1);
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

}
