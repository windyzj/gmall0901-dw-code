package com.atguigu.gmall0901.dw.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0901.dw.publisher.service.PublishService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
        }
        return  null;
    }

}
