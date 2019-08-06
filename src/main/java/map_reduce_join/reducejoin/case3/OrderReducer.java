package map_reduce_join.reducejoin.case3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<OrderBean> values,
			Context context) throws IOException, InterruptedException {
	    
		List<OrderBean>  orders = new ArrayList<>();
		OrderBean pd = new OrderBean() ;
		
		for (OrderBean orderBean : values) {
			
			if("order".equals(orderBean.getFlag())) {
				try {
					//来自于order表的数据
					OrderBean order  = new OrderBean();
					BeanUtils.copyProperties(order, orderBean);
					orders.add(order);
				} catch (Exception e) {
				}
					
			}else {
				try {
					//来自于pd表的数据
					BeanUtils.copyProperties(pd, orderBean);
				} catch (Exception e) {
					
				}
			}
		}
		
		// join 
		// 将pd对象中的pname属性值 赋值给 从orders集合中迭代出的每个TableBean对象的pname属性上.
		for (OrderBean orderBean : orders) {
			
			orderBean.setPname(pd.getPname());
			
			context.write(orderBean, NullWritable.get());
		}
		
	}
}
