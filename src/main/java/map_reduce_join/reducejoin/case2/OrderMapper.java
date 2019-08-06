package map_reduce_join.reducejoin.case2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

@SuppressWarnings("DuplicatedCode")
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private Text outputKey = new Text();
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1.判断当前key-value来自个文件，可以知道value的数据格式，做相应的切分
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split(" ");
        OrderBean orderBean;
        if (fileName.contains("order.txt")) {//当前处理的是订单文件
            orderBean = new OrderBean(split[0], split[1], Integer.parseInt(split[2]));
        } else {//当前处理的是品牌文件
            orderBean = new OrderBean(split[0], split[1]);

        }
        context.write(orderBean, NullWritable.get());
    }
}
