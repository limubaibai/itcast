package com.leyou;

import com.leyou.pojo.Goods;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface GoodsRepository extends ElasticsearchRepository<Goods,String > {
    List<Goods> findByTitle(String title);

    List<Goods> findByPriceBetween(int i, int i1);
}
