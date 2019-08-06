package topNSeries.grouping_solution;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import topNSeries.OrderBean;

import java.io.IOException;
import java.util.Iterator;

public class OrderGroupingReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

//        context.write(key, NullWritable.get());

        int i = 0;
        Iterator<NullWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            i++;
        }
        System.out.println(i);

        context.write(key, NullWritable.get());

    }
}
