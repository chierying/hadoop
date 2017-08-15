package com.zb.mapsidejoin.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by v_zhangbing on 2017/8/14.
 */
@Data
@AllArgsConstructor(staticName = "newInstance")
@NoArgsConstructor
public class Order implements Writable{
    private String orderId;
    private String productId;
    private int amount;
    private String productName;

    public void write(DataOutput dataOutput) throws IOException {

    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
