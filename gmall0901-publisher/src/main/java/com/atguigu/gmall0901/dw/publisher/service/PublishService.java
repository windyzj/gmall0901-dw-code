package com.atguigu.gmall0901.dw.publisher.service;

import java.util.Map;

public interface PublishService {

    public int getDauTotal(String date );

    public Map getDauHours(String date );

    public Map getNewOrderTotalAmountHours(String date);

    public Double getNewOrderTotalAmount(String date);

    public Map getSaleDetail(String date,String keyword,int startPage,int size,String aggField,int aggSize);

}
