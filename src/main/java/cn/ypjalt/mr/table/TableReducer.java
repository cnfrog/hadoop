package cn.ypjalt.mr.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> tableBeans = new ArrayList<TableBean>();
        TableBean tableBean = new TableBean();

        for (TableBean value : values) {
            if ("0".equals(value.getFlag())) {
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                tableBeans.add(orderBean);
            } else {
                try {
                    BeanUtils.copyProperties(tableBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // 3 表的拼接
        for (TableBean bean : tableBeans) {
            bean.setPname(tableBean.getPname());
            // 4 数据写出去
            context.write(bean, NullWritable.get());
        }

    }
}
