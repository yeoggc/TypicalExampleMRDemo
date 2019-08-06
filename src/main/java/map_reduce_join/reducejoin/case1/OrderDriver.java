package map_reduce_join.reducejoin.case1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 *
 * 使用自定义bean作为key，通过实现partitioner按照类中字段相同值分到同一个reduce,
 * 在通过分组实现某些字段分为一组交由reduce方法执行一次
 *
 */
public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"input/join", "output/join/o1"};


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

        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(2);
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

}
