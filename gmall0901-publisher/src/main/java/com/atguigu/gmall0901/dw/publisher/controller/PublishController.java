package com.atguigu.gmall0901.dw.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0901.dw.publisher.bean.Option;
import com.atguigu.gmall0901.dw.publisher.bean.Stat;
import com.atguigu.gmall0901.dw.publisher.service.PublishService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublishController {

    @Autowired
    PublishService publishService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){
        int dauTotal = publishService.getDauTotal(date);
        List totalList=new ArrayList();

        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);


        Map newMidMap=new HashMap();
        newMidMap.put("id","dau");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",1200);
        totalList.add(newMidMap);

        Double orderTotalAmount = publishService.getNewOrderTotalAmount(date);

        Map orderTotalMap=new HashMap();
        orderTotalMap.put("id","totalamount");
        orderTotalMap.put("name","新增交易额");
        orderTotalMap.put("value",orderTotalAmount);
        totalList.add(orderTotalMap);


        return   JSON.toJSONString(totalList);
    }


    @GetMapping("realtime-hours")
    public String  getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String date){

        if("dau".equals(id)){
            Map dauHoursMap=new HashMap();

            //获得每小时的日活
            Map dauHoursTodayMap = publishService.getDauHours(date);
            Date today=null;
            try {
                  today = new SimpleDateFormat("yyyy-MM-dd").parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Date yesterday = DateUtils.addDays(today, -1);
            String yesterdayDate = new SimpleDateFormat("yyyy-MM-dd").format(yesterday);
            Map dauHoursYesterdayMap = publishService.getDauHours(yesterdayDate);

            dauHoursMap.put("yesterday",dauHoursYesterdayMap);
            dauHoursMap.put("today",dauHoursTodayMap);

            return JSON.toJSONString(dauHoursMap);
        }else if("totalamount".equals(id)){
      //获得每小时的交易额
            Map totalamountHoursMap=new HashMap();

            Map totalamountHoursTodayMap = publishService.getNewOrderTotalAmountHours(date);
            //今天变昨天
            Date today=null;
            try {
                today = new SimpleDateFormat("yyyy-MM-dd").parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Date yesterday = DateUtils.addDays(today, -1);
            String yesterdayDate = new SimpleDateFormat("yyyy-MM-dd").format(yesterday);
            Map totalamountHoursYesterdayMap = publishService.getNewOrderTotalAmountHours(yesterdayDate);

            totalamountHoursMap.put("yesterday",totalamountHoursYesterdayMap);
            totalamountHoursMap.put("today",totalamountHoursTodayMap);

            return JSON.toJSONString(totalamountHoursMap);
        }
        return  null;
    }

    @GetMapping("sale_detail")
    public  String  getSaleDetail(HttpServletRequest request){
        int startpage=1;
        int size=20;

        String date = request.getParameter("date");
        String startpageStr = request.getParameter("startpage");
        if(startpageStr!=null){
            startpage=Integer.parseInt(startpageStr);
        }
        String sizeStr = request.getParameter("size");

        if(sizeStr!=null){
            size=Integer.parseInt(sizeStr);
        }
        String keyword = request.getParameter("keyword");

        Map saleGenderMap = publishService.getSaleDetail(date, keyword, startpage, size, "user_gender", 2);

        Integer total = (Integer)saleGenderMap.get("total");
        List statList=new ArrayList();

        //求 男女占比
        HashMap genderMap =(HashMap) saleGenderMap.get("agg");
        Long maleCount =(Long) genderMap.get("M");
        Long femaleCount =(Long) genderMap.get("F");
        Double maleRatio=  Math.round(1000.0*maleCount / total )/10.0;
        Double femaleRatio=  Math.round(1000.0*femaleCount / total )/10.0;
        List  genderOptions = new ArrayList();
        genderOptions.add(new   Option("男",maleRatio)) ;
        genderOptions.add(new   Option("女",femaleRatio)) ;

        statList.add(new Stat("性别占比",genderOptions)) ;

        //求 年龄占比
        Map saleAgeMap = publishService.getSaleDetail(date, keyword, startpage, size, "user_age", 100);

        HashMap ageMap =(HashMap) saleAgeMap.get("agg");
        Long age20Count =0L;
        Long age2030Count =0L;
        Long age30Count =0L;
        for (Object ageObj : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) ageObj;
            String ageStr = (String)entry.getKey();
            Long ageCount = (Long)entry.getValue();
            int age = Integer.parseInt(ageStr);
            if(age<20){
                age20Count+=ageCount;
            }else if(age>=20&&age<30){
                age2030Count+=ageCount;
            }else{
                age30Count+=ageCount;
            }

        }

        Double age20Ratio=  Math.round(1000.0*age20Count / total)/10.0;
        Double age2030Ratio=  Math.round(1000.0*age2030Count / total )/10.0;
        Double age30Ratio=  Math.round(1000.0*age30Count / total )/10.0;
        List  ageOptions = new ArrayList();
        ageOptions.add(new  Option("20岁以下",age20Ratio)) ;
        ageOptions.add(new  Option("20岁到30岁",age2030Ratio)) ;
        ageOptions.add(new  Option("30岁以上",age30Ratio)) ;

        statList.add(new Stat("年龄占比",ageOptions)) ;

        //集成数据  转化json
        Map saleMap=new HashMap();
        saleMap.put("total",total);
        saleMap.put("stat",statList);
        saleMap.put("detail",saleGenderMap.get("detail"));

        return JSON.toJSONString(saleMap);
    }



}
