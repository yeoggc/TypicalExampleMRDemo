package map_reduce_join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("DuplicatedCode")
public class MapJoinSample {


    public static void main(String[] args) throws Exception {

        args = new String[]{"input/map_reduce_join", "output/map_reduce_join/map_join"};

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);
        Path output = new Path(args[1]);
        fs.delete(output, true);

        Job job = Job.getInstance();
        job.addCacheFile(new URI("file:///Users/yeoggc/Documents/AtguiguCode/Hadoop/Hadoop_Study_GG/TypicalExampleMRDemo/input/map_reduce_join/pd.txt"));
        job.setJarByClass(MapJoinSample.class);

        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0] + "/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }

    static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        //pd表在内存中的缓存
        private Map<String, String> map = new HashMap<>();

        private Text outputKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //从缓存文件中找到pd.txt
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            //获取文件系统并开流
            FileSystem fs = FileSystem.get(context.getConfiguration());

            FSDataInputStream fsDataInputStream = fs.open(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split(" ");
                map.put(split[0], split[1]);
            }

            IOUtils.closeStream(fsDataInputStream);
            IOUtils.closeStream(br);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(" ");

            String pname = map.get(split[1]);

            outputKey.set(split[0] + "\t" + pname + "\t" + split[1]);
            context.write(outputKey, NullWritable.get());

        }
    }


}
