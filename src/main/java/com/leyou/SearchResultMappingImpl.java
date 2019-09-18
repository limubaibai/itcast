package com.leyou;

import com.alibaba.fastjson.JSON;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SearchResultMappingImpl implements SearchResultMapper {
    @Override
    public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {

        String scrollId = response.getScrollId();
        SearchHits hits = response.getHits();
        Aggregations aggregations = response.getAggregations();
        Long total = hits.totalHits;
        float maxScore = hits.getMaxScore();

        List<T> content=new ArrayList<>();
        SearchHit[] searchHits = response.getHits().getHits();
        for (SearchHit searchHit : searchHits) {
            String sourceAsString = searchHit.getSourceAsString();
            //转为对象
            T t = JSON.parseObject(sourceAsString, clazz);

            //获取高亮
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");
            Text[] fragments = highlightField.getFragments();
            if (fragments!=null&&fragments.length>0) {
                String title = fragments[0].toString();
                try {
                    BeanUtils.copyProperty(t,"title",title);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            content.add(t);
        }

        return new AggregatedPageImpl<T>(content,pageable,total,aggregations,scrollId,maxScore);
    }
}
