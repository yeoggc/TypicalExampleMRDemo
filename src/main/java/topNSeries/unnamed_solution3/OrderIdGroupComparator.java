package topNSeries.unnamed_solution3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import topNSeries.OrderBean;

public class OrderIdGroupComparator extends WritableComparator {

    public OrderIdGroupComparator() {
        //必须要调用父类构造器，否则抛空指针异常
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean p = (OrderBean) a;
        OrderBean r = (OrderBean) b;
        return Integer.compare(p.getOrder_id(), r.getOrder_id());
    }
}
