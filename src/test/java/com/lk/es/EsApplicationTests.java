package com.lk.es;

import com.alibaba.fastjson.JSON;
import com.lk.es.bean.User;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApplicationTests {
    @Autowired
    RestHighLevelClient restHighLevelClient;

    @SneakyThrows
    @Test
    void contextLoads() {

        //测试索引的创建 Request
        CreateIndexRequest request = new CreateIndexRequest("lk_index");
        //执行创建请求
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 测试获取索引
     */
    @SneakyThrows
    @Test
    void getExitIndex() {
        GetIndexRequest request = new GetIndexRequest("lk_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 测试删除索引
     */
    @SneakyThrows
    @Test
    void testDelIndex() {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("lk_index");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    /**
     * 测试添加文档
     */
    @SneakyThrows
    @Test
    void addDocument() {
        User user = new User("lk", 13);
        IndexRequest request = new IndexRequest("lk_index");
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);

        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }

    /**
     *  判断文档是否存在
     */
    @SneakyThrows
    @Test
    void testIsExitDocument() {

        GetRequest request = new GetRequest("lk_index", "1");
        //不获取返回的_source 的上下文
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 获取文档
     */
    @SneakyThrows
    @Test
    void getDocument() {
        GetRequest request = new GetRequest("lk_index", "1");
        GetResponse documentFields = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(documentFields.getSourceAsString());
        System.out.println(documentFields);
    }

    /**
     * 更新文档的信息
     */
    @SneakyThrows
    @Test
    void updateDocument() {
        UpdateRequest updateRequest = new UpdateRequest("lk_index", "1");
        updateRequest.timeout("1s");
        User user = new User("张三", 23);
        UpdateRequest doc = updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(doc, RequestOptions.DEFAULT);
        System.out.println(update);
        System.out.println(update.status());
    }
    /**
     *  删除文档
     */
    @SneakyThrows
    @Test
    void deleteRequest(){
        DeleteRequest deleteRequest = new DeleteRequest("lk_index", "1");
        deleteRequest.timeout("1s");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    /**
     *  批量插入文档
     */

    @SneakyThrows
    @Test
    void bulkRequest(){
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> users = new ArrayList<>();

        users.add(new User("张三5",12));
        users.add(new User("李四",22));
        users.add(new User("zhangsan",25));
        users.add(new User("lk",26));
        for (int i=0;i<users.size();i++){
//            bulkRequest.add(new IndexRequest("lk_index").id(""+(i+1))
//                    .source(JSON.toJSONString(users.get(i)),XContentType.JSON));
//不用id ，就会新生成默认的id
            bulkRequest.add(new IndexRequest("lk_index")
                    .source(JSON.toJSONString(users.get(i)),XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());
    }

    /**
     * 查询
     */
    @SneakyThrows
    @Test
    void search(){


        //构建搜素条件
        SearchSourceBuilder sourceBuilder  = new SearchSourceBuilder();
//        QueryBuilders.termQuery() //精确查询
//        QueryBuilders.matchAllQuery() 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", "张三");
        sourceBuilder.query(termQueryBuilder);

//        MatchAllQueryBuilder sourceBuilder = QueryBuilders.matchAllQuery();
//        sourceBuilder.query(matchAllQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
//        sourceBuilder.from(0);
//        sourceBuilder.size(10);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("lk_index");
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search);
        System.out.println("================================");
        System.out.println(JSON.toJSONString(search.getHits()));
        for (SearchHit searchHit: search.getHits().getHits()){
            System.out.println(searchHit.getSourceAsMap());
        }
    }

}
