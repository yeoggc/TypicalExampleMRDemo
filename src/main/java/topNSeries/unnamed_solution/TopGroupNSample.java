package topNSeries.unnamed_solution;

import org.apache.commons.beanutils.BeanUtils;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 思路：
 * map端：
 * 输出key为 order_id
 * 输出Value为OrderBean对象，存储最终需要输出到文件的数据，toString为具体输出到文本的格式数据
 * reduce端
 * 同一 order_id 会到同一个reduce中，并会执行一次reduce方法，
 * 输入的value集合为对应 order_id 的所有元素
 * <p>
 * 把values的转为成List，并排序，最后遍历输出
 * <p>
 * 注意：Iterable<OrderBean> values 这个迭代出的orderBean对象是重用的
 */
class TopGroupNSample {

    private static final String ORDER_AMOUNT_TOP_N_NUM = "order_amount_top_n_num";

    public static void main(String[] args) throws Exception {
        args = new String[]{"input/topNSeries", "output/topNSeries/unnamed_solution/o1"};
        Configuration configuration = new Configuration();
        //此参数可以控制最终输出top数量为 用户自定义
        configuration.setInt(ORDER_AMOUNT_TOP_N_NUM, 1);
        Job job = Job.getInstance(configuration);

        FileSystem fs = FileSystem.get(configuration);
        fs.delete(new Path(args[1]), true);

        job.setJarByClass(TopGroupNSample.class);
        job.setMapperClass(TopGroupNMapper.class);
        job.setReducerClass(TopGroupNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : -1);
    }


    static class TopGroupNMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
        private Text outputKey = new Text();
        private OrderBean outputValue = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");

            outputKey.set(split[0]);
            outputValue.setOrder_id(Integer.parseInt(split[0]));
            outputValue.setPrice(Double.parseDouble(split[2]));

            context.write(outputKey, outputValue);

        }
    }

    static class TopGroupNReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {

        private List<OrderBean> orderBeanList = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
            Iterator<OrderBean> iterator = values.iterator();
            orderBeanList.clear();

            //获取在Driver端，设置配置参数
            int topNNum = context.getConfiguration().getInt(ORDER_AMOUNT_TOP_N_NUM, 1);

            values.forEach(orderBean -> {
                //构造一个新的对象，来存储本次迭代出来的值
                try {
                    OrderBean newOrderBean = (OrderBean) BeanUtils.cloneBean(orderBean);
                    orderBeanList.add(newOrderBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            //对List进行排序
            Collections.sort(orderBeanList);

            //防止topNNum大于商品数量
            int realTopNNum = Math.min(topNNum, orderBeanList.size());

            for (int i = 0; i < realTopNNum; i++) {
                context.write(orderBeanList.get(i), NullWritable.get());
            }

        }
    }


}
