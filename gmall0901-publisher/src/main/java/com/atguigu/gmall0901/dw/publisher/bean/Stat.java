package com.atguigu.gmall0901.dw.publisher.bean;

import java.util.ArrayList;
import java.util.List;

public class Stat {

       String title;

       List<Option> options= new ArrayList();

       public Stat(String title,List options){
           this.title=title;
           this.options=options;
       }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }
}
