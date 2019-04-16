package com.example.elasticsearchrest.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Index {

    String value() default "_index";

    String idField() default "id";
}
