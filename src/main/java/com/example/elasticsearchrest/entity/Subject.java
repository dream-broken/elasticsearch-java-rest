package com.example.elasticsearchrest.entity;

import com.example.elasticsearchrest.annotation.Index;
import lombok.Data;

@Data
@Index(value = "build", idField = "SId")
public class Subject {

    private  long SId;


    private  long EId;


    private  long STId;


    private  String Result;


    private  String Point;


    private  long ETId;


    private  long EOId;


    private  String Title;


    private  String Content;


    private  String Analysis;
}
