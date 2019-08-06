package map_reduce_join.reducejoin.case1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {


    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //第一条数据来自pd，之后全部来自order
        Iterator<NullWritable> iterator = values.iterator();


        //通过第一条数据获取pname
        iterator.next();
        String pname = key.getPname();


        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
