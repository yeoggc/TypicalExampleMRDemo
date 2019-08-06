package map_reduce_join.reducejoin.case2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class OrderGroupingComparator extends WritableComparator {

    OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean p = (OrderBean) a;
        OrderBean r = (OrderBean) b;

        return p.getPid().compareTo(r.getPid());
    }
}
