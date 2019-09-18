package com.leyou;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.leyou.pojo.Item;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsManager {
    private RestHighLevelClient client=null;
    private Gson gson=new Gson();

    @Before
    public  void  init(){//建立客户端连接   并搭建集群
        client=new RestHighLevelClient(RestClient.builder(
                new HttpHost("127.0.0.1",9201,"http"),
                new HttpHost("127.0.0.1",9202,"http"),
                new HttpHost("127.0.0.1",9203,"http")
        ));
    }

    //向ElasticSearch中添加或修改数据
    @Test
    public  void  testSaveDoc() throws Exception {
        Item item=new Item("2","华为P30手机","手机","华为",3899.0,"q3311");
        //创建插入索引数据 的对象 indexRequest
        IndexRequest request=new IndexRequest("item","docs",item.getId());
        //将Java对象转为json字符串
        String jsonString = JSON.toJSONString(item);//fastJson转json
//        String jsonString = gson.toJson(item);//gson转json
        //为请求对象添加  数据
        request.source(jsonString, XContentType.JSON);
        //客户端进行添加或修改
        client.index(request, RequestOptions.DEFAULT);
    }

//删除数据
    @Test
    public  void  testDeleteDoc() throws Exception {
        //删除item 索引库中  docs类型的id为1的数据
        DeleteRequest deleteRequest=new DeleteRequest("item","docs","2");
        client.delete(deleteRequest,RequestOptions.DEFAULT);
    }


//批量新增
    @Test
    public  void  testBulkAddDoc() throws Exception {
        List<Item> list = new ArrayList<>();
        list.add(new Item("1", "小米手机7", "手机", "小米", 3299.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("2", "坚果手机R1", "手机", "锤子", 3699.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("3", "华为META10", "手机", "华为", 4499.00,"http://image.leyou.com/13123.jpg"));
        list.add(new Item("4", "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item("5", "荣耀V10", "手机", "华为", 2799.00,"http://image.leyou.com/13123.jpg"));

        //创建批量新增请求对象
        BulkRequest request=new BulkRequest();

        list.forEach(item -> {//lambda表达式进行遍历
            //创建用于新增数据的请求对象
            IndexRequest indexRequest=new IndexRequest("item","docs",item.getId());
            //Java对象转为json字符串
            String jsonString = JSON.toJSONString(item);
            //将数据添加到请求中
            indexRequest.source(jsonString,XContentType.JSON);
            //将单个新增请求对象添加到批量添加对象中
            request.add(indexRequest);

        });

        //执行批量添加
        client.bulk(request,RequestOptions.DEFAULT);

    }

//各种查询
    @Test
    public  void  testSearchDoc() throws Exception {
        //创建查询请求对象
        SearchRequest searchRequest=new SearchRequest("item").types("docs");
        //构建查询方式对象
        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        //term查询
        searchSourceBuilder.query(QueryBuilders.termQuery("title","手机"));

        //查询所有
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //通配符查询
//        searchSourceBuilder.query(QueryBuilders.wildcardQuery("title","*手机*"));

        //分词查询
//        searchSourceBuilder.query(QueryBuilders.matchQuery("title","小米手机7"));

        //过滤
//        searchSourceBuilder.fetchSource(new String[]{"title","price"},null);//显示字段的过滤
       //数据过滤   过滤出品牌为锤子的手机
//        searchSourceBuilder.postFilter(QueryBuilders.termQuery("brand","锤子"));

        //分页
        /*searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);*/

//        排序
        searchSourceBuilder.sort("price", SortOrder.DESC);

        //高亮
        //构建高亮条件
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.field("title");//高亮字段
        highlightBuilder.preTags("<span style='color:red'>");//高亮效果
        highlightBuilder.postTags("</span>");
        //添加高亮查询
        searchSourceBuilder.highlighter(highlightBuilder);

        //放入查询请求对象中
        searchRequest.source(searchSourceBuilder);
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询总结果数为："+hits.totalHits);

        //获取查询结果
        SearchHit[] searchHits = hits.getHits();
        //遍历查询结果
        for (SearchHit searchHit : searchHits) {
            //获取json字符串结果
            String jsonString = searchHit.getSourceAsString();
//            System.out.println(jsonString);
            //json字符串转为Java对象
            Item item = JSON.parseObject(jsonString,Item.class);//使用fastjson
            Item item1 = gson.fromJson(jsonString, Item.class); //使用gson

            //获取高亮效果
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");
            System.out.println(highlightField);

            Text[] fragments = highlightField.getFragments();
            if (fragments!=null&&fragments.length>0){
                String title = fragments[0].toString();
                item.setTitle(title);
            }
            System.out.println(item);
        }
    }

    //聚合测试
    @Test
    public  void  testAggDoc() throws Exception {
        //创建查询请求对象
       SearchRequest searchRequest=new SearchRequest("item").types("docs");
       //构造查询条件
        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //构造聚合条件
        searchSourceBuilder.aggregation(AggregationBuilders.terms("brandCount").field("brand"));
        searchRequest.source(searchSourceBuilder);

        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取聚合结果aggregations
        Aggregations aggregations = searchResponse.getAggregations();
//获取aggregations中名为brandCount的对象
        Terms terms = aggregations.get("brandCount");
        //获取brandCount中的buckets对象
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println(bucket.getKeyAsString()+":"+bucket.getDocCount());
        });
       /* SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String jsonString = hit.getSourceAsString();
            //转为对象
            Item item = gson.fromJson(jsonString, Item.class);
            System.out.println(item);
        }*/

    }

    @After
    public void end() throws Exception {
        client.close();
    }

}
