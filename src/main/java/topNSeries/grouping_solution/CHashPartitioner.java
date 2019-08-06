package topNSeries.grouping_solution;

import org.apache.hadoop.mapreduce.Partitioner;

public class CHashPartitioner<K, V> extends Partitioner<K, V> {
    public int getPartition(K key, V value,
                            int numReduceTasks) {
        int hashCode = key.hashCode();
        int i = hashCode & Integer.MAX_VALUE;
        int num = i % numReduceTasks;
        return num;
    }
}
