package map_reduce_join.reducejoin.case3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, OrderBean> {

    Text k = new Text();
    OrderBean v = new OrderBean();
    private String fileName;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        // 获取切片对象，获取到文件信息
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;

        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //1.读取一行数据
        String line = value.toString();

        //2.切割
        String[] fields = line.split("\t");

        //3.封装对象,根据当前的切片信息判断数据来自于哪个文件，以决定每条数据切割完以后生成几个字段.
        if (fileName.contains("order")) {
            //订单表
            //  1001	01	1

            k.set(fields[1]);

            v.setOrder_id(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setFlag("order");
            v.setPname("");

        } else if (fileName.contains("pd")) {
            //产品表
            //01	小米
            k.set(fields[0]);

            v.setOrder_id("");
            v.setPid(fields[0]);
            v.setPname(fields[1]);
            v.setFlag("pd");
            v.setAmount(0);

        }

        //4. 写出
        context.write(k, v);
    }
}
