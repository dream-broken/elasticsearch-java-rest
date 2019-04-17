package com.example.elasticsearchrest.entity;

import com.example.elasticsearchrest.annotation.Index;
import lombok.Data;

import java.lang.reflect.Field;

@Data
public class Document {

    private String index;
    private String id;

    public Document(String index, String id) {
        this.index = index;
        this.id = id;
    }

    public static Document getDocument(Object entity) throws Exception {
        Class clazz = entity.getClass();
        if (clazz.isAnnotationPresent(Index.class)) {
            Index index = (Index)clazz.getAnnotation(Index.class);

            Field field = clazz.getDeclaredField(index.idField());
            field.setAccessible(true);
            String id = field.get(entity).toString();

            return new Document(index.value(), id);
        }
        else {
            throw new Exception("实体类不符");
        }
    }

    public static String getIndexValue(Class clazz) throws Exception {
        if (clazz.isAnnotationPresent(Index.class)) {
            Index index = (Index)clazz.getAnnotation(Index.class);
            return index.value();
        }
        else {
            throw new Exception("实体类不符");
        }
    }

}
