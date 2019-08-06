package topNSeries.unnamed_solution2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import topNSeries.OrderBean;

import java.io.IOException;

/**
 * 思路：
 *  到达reduce端后，数据是有序的，且金额是从大到小的，
 *  可以**不使用分组**，在reduce端利用变量记录第一次出现的订单ID,以后相同的id忽略
 */
 class TopGroupNSample {
    private static final String ORDER_AMOUNT_TOP_N_NUM = "order_amount_top_n_num";

    public static void main(String[] args) throws Exception {

        args = new String[]{"input/topNSeries", "output/topNSeries/unnamed_solution2/o1"};

        Configuration configuration = new Configuration();
        configuration.setInt(ORDER_AMOUNT_TOP_N_NUM, 1);

        FileSystem fs = FileSystem.get(configuration);
        fs.delete(new Path(args[1]), true);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(TopGroupNSample.class);
        job.setMapperClass(TopGroupNMapper.class);
        job.setReducerClass(TopGroupNReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(2);
        job.setPartitionerClass(OrderBeanPartitioner.class);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);

    }

    static class TopGroupNMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        private OrderBean orderBean = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");

            orderBean.setOrder_id(Integer.parseInt(split[0]));
            orderBean.setPrice(Double.parseDouble(split[2]));

            context.write(orderBean, NullWritable.get());

        }
    }

    static class TopGroupNReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        int order_id = -1;
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int newOrderId = key.getOrder_id();

            if(order_id != newOrderId){
                context.write(key, NullWritable.get());
            }
            order_id = newOrderId;
        }
    }

}
