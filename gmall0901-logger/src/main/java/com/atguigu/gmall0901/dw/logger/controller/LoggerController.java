package com.atguigu.gmall0901.dw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0901.dw.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;



@RestController  // =Controller+Responsebody
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("log")
    public  String  log(@RequestParam("log") String logJson){
        System.out.println(logJson);
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts",System.currentTimeMillis());

        sendKafka(jsonObject);

        logger.info(jsonObject.toJSONString());

        return   "success";
    }

    private void  sendKafka(JSONObject jsonObject){
        //发送kafka  1、kafka producer  2、 kafka template(springboot 和 kafka的整合)
        // 根据logJson中type  来分到不同的topic中
        if(jsonObject.getString("type").equals("startup")){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }

    }

}
