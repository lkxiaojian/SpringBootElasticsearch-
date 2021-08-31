package com.lk.es.service;

import com.alibaba.fastjson.JSON;
import com.lk.es.bean.Content;
import com.lk.es.utli.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    public Boolean parseContent(String keyword) {
        List<Content> contents = new HtmlParseUtil().parseJD(keyword);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");

        for (Content content : contents) {
            bulkRequest.add(new IndexRequest("jd_goods").source(JSON.toJSONString(content), XContentType.JSON));
        }


        try {
            BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            return !bulk.hasFailures();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public List<Map<String,Object>> getContents(String keyword,int pageNo,int pageSize) {
        if(pageNo<0){
            pageNo=0;
        }
        if(pageSize<1){
            pageSize=10;
        }

        SearchSourceBuilder sourceBuilder  = new SearchSourceBuilder();
//        QueryBuilders.termQuery() //精确查询
//        QueryBuilders.matchAllQuery() 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.from(pageNo);
        sourceBuilder.size(pageSize);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("jd_goods");
        searchRequest.source(sourceBuilder);
        SearchResponse response = null;
        try {
            response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ArrayList<Map<String, Object>> maps = new ArrayList<>();
        System.out.println(JSON.toJSONString(response.getHits()));
        for (SearchHit searchHit: response.getHits().getHits()){
            maps.add(searchHit.getSourceAsMap());
        }


        return maps;

    }

    public List<Map<String,Object>> getHighContents(String keyword,int pageNo,int pageSize) {
        if(pageNo<0){
            pageNo=0;
        }
        if(pageSize<1){
            pageSize=10;
        }

        SearchSourceBuilder sourceBuilder  = new SearchSourceBuilder();
//        QueryBuilders.termQuery() //精确查询
//        QueryBuilders.matchAllQuery() 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", keyword);
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.from(pageNo);
        sourceBuilder.size(pageSize);
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.requireFieldMatch(false);
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        sourceBuilder.highlighter(highlightBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("jd_goods");
        searchRequest.source(sourceBuilder);
        SearchResponse response = null;
        try {
            response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ArrayList<Map<String, Object>> maps = new ArrayList<>();
        System.out.println(JSON.toJSONString(response.getHits()));
        for (SearchHit searchHit: response.getHits().getHits()){
            //解析高亮的字段
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();

            if(title!=null){
                Text[] fragments = title.getFragments();

                StringBuilder newTile= new StringBuilder();
                for (Text fragment : fragments) {
                    newTile.append(fragment);
                }
                sourceAsMap.put("title", newTile.toString());
            }
            maps.add(searchHit.getSourceAsMap());
        }
        return maps;

    }
}
