package cn.ypjalt.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int order_id; // 订单id号
    private double price; // 价格


    public OrderBean() {
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id +
                "\t" + price;
    }

    public int compareTo(OrderBean o) {
        int result;
        if (order_id > o.getOrder_id())
            result = 1;
        else if (order_id < o.getOrder_id())
            result = -1;
        else
            result = this.price > o.getPrice() ? -1 : 1;
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {

    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
