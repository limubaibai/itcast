package com.leyou;


import com.leyou.pojo.Goods;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringDataEsTest {


    @Autowired
    private ElasticsearchTemplate esTemplate;
    @Autowired
    private GoodsRepository goodsRepository;

    //根据实体类Goods中的注解配置生成索引库
    @Test
    public void testAddIndex(){
        esTemplate.createIndex(Goods.class);
    }

    //生成映射关系
    @Test
    public void testAddMapping(){
        esTemplate.putMapping(Goods.class);
    }

    //添加文档
    @Test
    public void testAddDoc(){
        Goods goods=new Goods("1","小米9手机","手机","小米",1199.0,"q3311");
        goodsRepository.save(goods);//新增和修改
    }



    //删除文档
    @Test
    public void testDeleteDoc(){
        goodsRepository.deleteById("1");//删除id为1的
//        goodsRepository.deleteAll();//删除所有
    }

    //批量新增
@Test
    public void testAddBulkDoc(){
    List<Goods> list = new ArrayList<>();
    list.add(new Goods("1", "小米手机7", "手机", "小米", 3299.00,"http://image.leyou.com/13123.jpg"));
    list.add(new Goods("2", "坚果手机R1", "手机", "锤子", 3699.00,"http://image.leyou.com/13123.jpg"));
    list.add(new Goods("3", "华为META10", "手机", "华为", 4499.00,"http://image.leyou.com/13123.jpg"));
    list.add(new Goods("4", "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
    list.add(new Goods("5", "荣耀V10", "手机", "华为", 2799.00,"http://image.leyou.com/13123.jpg"));
    goodsRepository.saveAll(list);
    }


    //自定义查询
    @Test
    public void testSearchDoc(){
//        List<Goods> goods = goodsRepository.findByTitle("小米");

        List<Goods> goods=goodsRepository.findByPriceBetween(2000,4000);
        goods.forEach(good->{
            System.out.println(good);
        });
    }


    //结合原生API查询
    @Test
    public void testQueryDoc(){
        NativeSearchQueryBuilder nativeSearchQueryBuilder=new NativeSearchQueryBuilder();
        //查询方式
//        nativeSearchQueryBuilder.withQuery(QueryBuilders.rangeQuery("price").gte(2000).lte(4000));

        //nativeSearchQueryBuilder.withQuery(QueryBuilders.matchQuery("title","坚果手机R1"));
        nativeSearchQueryBuilder.withQuery(QueryBuilders.termQuery("title","小米"));
        //分页
        //nativeSearchQueryBuilder.withPageable(PageRequest.of(0,2));
       /* //聚合条件
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("brandCount").field("brand"));

        AggregatedPage<Goods> aggregatedPage = esTemplate.queryForPage(nativeSearchQueryBuilder.build(), Goods.class);
        //获取聚合结果
        Terms brandCount = (Terms) aggregatedPage.getAggregation("brandCount");
        List<? extends Terms.Bucket> buckets = brandCount.getBuckets();
        buckets.forEach(bucket->{
            //遍历打印每个品牌的数量
            System.out.println(bucket.getKeyAsString()+":"+bucket.getDocCount());
        });*/

       //构造高亮条件
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        nativeSearchQueryBuilder.withHighlightBuilder(highlightBuilder);
        nativeSearchQueryBuilder.withHighlightFields(new HighlightBuilder.Field("title"));


        //获取高亮结果
        AggregatedPage<Goods> aggregatedPage = esTemplate.
                queryForPage(nativeSearchQueryBuilder.build(), Goods.class, new SearchResultMappingImpl());


        //查询结果
        List<Goods> goodsList = aggregatedPage.getContent();
        goodsList.forEach(goods -> {
            System.out.println(goods);
        });


    }
}
