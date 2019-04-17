package com.example.elasticsearchrest;

import com.example.elasticsearchrest.dao.AbstractBaseDao;
import com.example.elasticsearchrest.dao.SubjectDao;
import com.example.elasticsearchrest.entity.Document;
import com.example.elasticsearchrest.entity.Subject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchRestApplicationTests {

    @Resource
    private SubjectDao subjectDao;

    @Test
    public void contextLoads() throws Exception {
        Subject subject = new Subject(2, 2, 1, "1", "1", 1, 1, "1", "1", "1");

        String save = subjectDao.save(subject);
        System.out.println(save);
    }

    @Test
    public void del() throws Exception {
        //String build = subjectDao.delete("build", "1");

        String delete = subjectDao.delete(new Subject(2));
        System.out.println(delete);
    }

    @Test
    public void typeTest() throws Exception {
        Class<?> componentType = subjectDao.getClass().getComponentType();
        String typeName = subjectDao.getClass().getTypeName();
        TypeVariable<? extends Class<? extends SubjectDao>>[] typeParameters = subjectDao.getClass().getTypeParameters();
        System.out.println(componentType);
        System.out.println(typeName);
        System.out.println(typeParameters);
        Object genericInfo = subjectDao.getClass().getGenericSuperclass();
        ParameterizedType abstractBaseDao = (ParameterizedType) genericInfo;
        System.out.println(abstractBaseDao.getActualTypeArguments());
        System.out.println(abstractBaseDao.getTypeName());
        System.out.println(abstractBaseDao.getRawType());
        Class<Subject> c = (Class<Subject>)abstractBaseDao.getActualTypeArguments()[0];
        String indexValue = Document.getIndexValue(c);
        System.out.println(indexValue);
    }

}
