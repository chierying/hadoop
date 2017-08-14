package com.zb.mrjoin.po;

import lombok.Data;

/**
 * Created by v_zhangbing on 2017/8/14.
 */
@Data
public class Order {
    private String orderId;
    private String productId;
    private int amount;
}
