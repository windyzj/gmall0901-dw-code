package com.atguigu.gmall0901.dw.publisher.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0901.dw.common.constant.GmallConstant;
import com.atguigu.gmall0901.dw.publisher.service.PublishService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublishServiceImpl implements PublishService {

    @Autowired
    JestClient jestClient;

    @Override
    public int getDauTotal(String date) {
        int total=0;
/*        String query="{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"match\":{\n" +
                "          \"logDate\":\"2019-03-04\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";*/
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("logDate", date);

        boolQueryBuilder.filter(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            total=searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return total;
    }

    @Override
    public Map getDauHours(String date) {

        Map dauHourMap=new HashMap();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("logDate", date);

        boolQueryBuilder.filter(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour.keyword").size(24);
        searchSourceBuilder.aggregation(termsBuilder);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();


        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> entryList = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry entry : entryList) {
                dauHourMap .put(entry.getKey(),entry.getCount());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return dauHourMap;
    }




    public Map getNewOrderTotalAmountHours(String date){
        Map totalAmountHoursMap =new HashMap();

//        GET gmall0901_new_order/_search
//        {
//            "query": {
//            "bool": {
//                "filter": {
//                    "match":{
//                        "createDate":"2019-03-06"
//                    }
//                }
//            }
//        }
//  , "aggs": {
//            "groupby_createHour": {
//                "terms": {
//                    "field": "createHour",
//                            "size": 24
//                },
//                "aggs": {
//                    "sum_totalamount": {
//                        "sum": {
//                            "field": "totalAmount"
//                        }
//                    }
//                }
//            }
//        }
//        }


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤部分
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new MatchQueryBuilder("createDate",date));

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合部分
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);

        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        //子聚合
        termsBuilder.subAggregation(sumBuilder);

        searchSourceBuilder.aggregation(termsBuilder);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();


        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Double sumTotalamount = bucket.getSumAggregation("sum_totalamount").getSum();
                totalAmountHoursMap.put(key,sumTotalamount);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(totalAmountHoursMap.toString());
        return totalAmountHoursMap;
    }


    public Double getNewOrderTotalAmount(String date){
        Map map = getNewOrderTotalAmountHours(date);
        Double allTotalAmount=0D;
        for (Object amount : map.values()) {
            Double totalAmount = (Double) amount;
            allTotalAmount+=totalAmount;
        }
        return allTotalAmount;
    }


    public Map getSaleDetail(String date,String keyword,int startPage,int size,String aggField,int aggSize){
             //  求总数 ，求明细 ，求分组结果
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //过滤时间
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new MatchQueryBuilder("dt",date));
        //匹配关键词
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        searchSourceBuilder.size(size);  //页行数
        searchSourceBuilder.from((startPage-1)*size); //起始行标  = （起始页码-1 ）* 页行数

        TermsBuilder termsAggsBuilder = AggregationBuilders.terms("groupby_" + aggField).field(aggField).size(aggSize);
        searchSourceBuilder.aggregation(termsAggsBuilder);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType(GmallConstant.ES_DEFAULT_TYPE).build();


        Map saleMap=new HashMap();
        Integer total=0;
        List saleDetailList=new ArrayList();
        Map  aggMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //取总数
            total = searchResult.getTotal();
            //取明细
            List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
            for (SearchResult.Hit<HashMap, Void> hit : hits) {
                HashMap saleDetail = hit.source;
                saleDetailList.add(saleDetail) ;
            }
            //取分组结果
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggField).getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                aggMap.put( bucket.getKey(),bucket.getCount());
            }
            //集成
            saleMap.put("total",total);
            saleMap.put("detail",saleDetailList);
            saleMap.put("agg",aggMap);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return saleMap;
    }

}
