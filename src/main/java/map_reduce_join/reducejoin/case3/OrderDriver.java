package map_reduce_join.reducejoin.case3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 通过在自定义bean增加标识，来区别是哪个文件的数据实体bean
 *
 * 在reduce端，相同的pid会被分配在一起并执行一次reduce方法，
 * 在这过程中，用一个集合存储order类型数据，一个bean对象存储pd类型数据
 * 遍历集合并设置品牌即可
 */
@SuppressWarnings("DuplicatedCode")
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"input/map_reduce_join", "output/map_reduce_join/case3"};

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

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }

}
