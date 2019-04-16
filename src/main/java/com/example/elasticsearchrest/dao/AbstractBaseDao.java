package com.example.elasticsearchrest.dao;

import com.example.elasticsearchrest.annotation.Index;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Resource;

abstract public class AbstractBaseDao<T> {

    @Resource
    protected RestHighLevelClient restHighLevelClient;

    public String save(T entity) throws Exception {
        IndexResponse index = restHighLevelClient.index(this.indexRequest(entity), RequestOptions.DEFAULT);
        return index.getId();
    }

    protected IndexRequest indexRequest(T entity) throws Exception {
        Class clazz = entity.getClass();
        if (clazz.isAnnotationPresent(Index.class)) {
            Index index = (Index)clazz.getAnnotation(Index.class);
            String id = clazz.getDeclaredField(index.idField()).get(entity).toString();
            return new IndexRequest(index.value()).id(id);
        }
        else {
            throw new Exception("实体类不符");
        }
    }
}
