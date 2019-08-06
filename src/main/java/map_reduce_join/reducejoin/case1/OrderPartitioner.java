package map_reduce_join.reducejoin.case1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        int partition = (orderBean.getPid().hashCode() & Integer.MAX_VALUE) % numPartitions;

        return partition;
    }
}
