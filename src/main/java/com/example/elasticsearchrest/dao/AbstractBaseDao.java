package com.example.elasticsearchrest.dao;

import com.alibaba.fastjson.JSON;
import com.example.elasticsearchrest.entity.Document;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

abstract public class AbstractBaseDao<T> {

    @Resource
    protected RestHighLevelClient restHighLevelClient;

    private String index;

    private Class<T> clazz;

    public String save(T entity) throws Exception {
        Document document = Document.getDocument(entity);
        IndexRequest indexRequest = new IndexRequest(document.getIndex()).id(document.getId()).source(JSON.toJSONString(entity), XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        return index.status().name();
    }

    public String delete(String id) throws Exception {
        return this.delete(this.getIndex(), id);
    }

    public String delete(String index, String id) throws Exception {
        DeleteResponse delete = restHighLevelClient.delete(new DeleteRequest(index, id), RequestOptions.DEFAULT);
        return delete.status().name();
    }

    public String delete(T entity) throws Exception {
        Document document = Document.getDocument(entity);
        return this.delete(document.getIndex(), document.getId());
    }

    public List<T> all() throws Exception {
        return this.all(0, 100);
    }

    public List<T> all(int num, int size) throws Exception {
        SearchRequest searchRequest = this.setSource(new SearchSourceBuilder().from(num).size(size));
        return this.analysis(this.searchResponse(searchRequest));
    }

    public List<T> all(int num, int size, String sort) throws Exception {
        SearchRequest searchRequest = this.setSource(new SearchSourceBuilder().from(num).size(size).sort(sort));
        return this.analysis(this.searchResponse(searchRequest));
    }

    public List<T> query(QueryBuilder queryBuilder) throws Exception {
        return this.query(queryBuilder, 0, 5);
    }

    public List<T> query(QueryBuilder queryBuilder, int num, int size) throws Exception {
        SearchRequest searchRequest = this.setSource(new SearchSourceBuilder().from(num).size(size).query(queryBuilder));
        return this.analysis(this.searchResponse(searchRequest));
    }

    public List<T> query(QueryBuilder queryBuilder, int num, int size, String sort) throws Exception {
        SearchRequest searchRequest = this.setSource(new SearchSourceBuilder().from(num).size(size).sort(sort).query(queryBuilder));
        return this.analysis(this.searchResponse(searchRequest));
    }

    public Aggregations aggregations(AggregationBuilder aggregation) throws Exception {
        SearchRequest searchRequest = this.setSource(new SearchSourceBuilder().aggregation(aggregation));
        return this.searchResponse(searchRequest).getAggregations();
    }

    public long count() throws Exception {
        return this.countRequest(new CountRequest(this.getIndex())).getCount();
    }

    private SearchResponse searchResponse(SearchRequest searchRequest) throws Exception {
        return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    private CountResponse countRequest(CountRequest countRequest) throws Exception {
        return restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
    }

    protected SearchRequest setSource(SearchSourceBuilder searchSourceBuilder) throws Exception {
        return new SearchRequest(this.getIndex()).source(searchSourceBuilder);
    }

    protected String getIndex() throws Exception {
        if (index == null) {
            Class<T> clazz = this.getEntityClazz();
            index = Document.getIndexValue(clazz);
        }
        return index;
    }

    private Class<T> getEntityClazz() {
        if (this.clazz == null) {
            Object genericInfo = this.getClass().getGenericSuperclass();
            ParameterizedType parameterizedType = (ParameterizedType) genericInfo;
            this.clazz = (Class<T>) parameterizedType.getActualTypeArguments()[0];
        }
        return this.clazz;
    }

    private List<T> analysis(SearchResponse searchResponse) throws Exception {
        List<T> data = new ArrayList<>();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            T entity = this.clazz.newInstance();
            BeanUtils.populate(entity, hit.getSourceAsMap());
            data.add(entity);
        }
        return data;
    }
}
