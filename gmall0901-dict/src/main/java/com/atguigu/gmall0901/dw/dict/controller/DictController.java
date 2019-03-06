package com.atguigu.gmall0901.dw.dict.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.Date;

@RestController
public class DictController {

    @GetMapping("dict")
    public String dict(HttpServletResponse response){
        //从 数据库中 读取 当前维护的自定义分词
        String dict="流浪地球\n飞驰人生\n双卡双待";
        response.addHeader("Last-Modified",new Date().toString());
        return dict;
    }
}
