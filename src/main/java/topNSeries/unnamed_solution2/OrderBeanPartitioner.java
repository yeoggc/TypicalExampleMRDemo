package topNSeries.unnamed_solution2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import topNSeries.OrderBean;

class OrderBeanPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        int partition = (orderBean.getOrder_id()&Integer.MAX_VALUE) % numPartitions;
        return partition;
    }
}
