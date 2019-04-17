package com.example.elasticsearchrest.entity;

import com.example.elasticsearchrest.annotation.Index;
import lombok.Data;

@Data
@Index(value = "build", idField = "id")
public class Subject {

    private  long id;

    private  long groupId;

    private  long type;

    private  String result;

    private  String grade;

    private  long realTopicId;

    private  String title;

    private  String content;

    private  String analysis;

    public Subject(long SId, long EId, long STId, String result, String point, long ETId, long EOId, String title, String content, String analysis) {
        this.id = SId;
        this.groupId = EId;
        this.type = STId;
        this.result = result;
        this.grade = point;
        this.realTopicId = ETId;
        this.title = title;
        this.content = content;
        this.analysis = analysis;
    }

    public Subject(long id) {
        this.id = id;
    }

    public Subject() {
    }
}
