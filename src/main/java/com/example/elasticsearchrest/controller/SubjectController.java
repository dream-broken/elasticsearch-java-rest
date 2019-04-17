package com.example.elasticsearchrest.controller;

import com.example.elasticsearchrest.dao.SubjectDao;
import com.example.elasticsearchrest.entity.Subject;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/subject")
public class SubjectController {

    @Resource
    private SubjectDao subjectDao;

    @GetMapping("/all")
    public List<Subject> all() throws Exception {
        return subjectDao.all();
    }

    @GetMapping("/all_page/{num}/{size}")
    public List<Subject> allPage(@PathVariable int num, @PathVariable int size) throws Exception {
        return subjectDao.all(num, size);
    }

    @GetMapping("/all_page_sort/{num}/{size}/{sort}")
    public List<Subject> allPageSort(@PathVariable int num, @PathVariable int size, @PathVariable String sort) throws Exception{
        return subjectDao.all(num, size, sort);
    }

    @GetMapping("/match/{title}")
    public List<Subject> match(@PathVariable String title) throws Exception {
        return subjectDao.query(QueryBuilders.matchQuery("title", title));
    }

    @GetMapping("/match_phrase/{title}")
    public List<Subject> matchPhrase(@PathVariable String title) throws Exception {
        return subjectDao.query(QueryBuilders.matchPhraseQuery("title", title));
    }

    @GetMapping("/like_title_and_analysis/{title}/{analysis}")
    public List<Subject> likeTitleAndAnalysis1(@PathVariable String title, @PathVariable String analysis) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", title)).must(QueryBuilders.matchQuery("analysis", analysis));
        return subjectDao.query(query);
    }

    @GetMapping("/like_title_or_analysis/{title}/{analysis}")
    public List<Subject> likeTitleOrAnalysis1(@PathVariable String title, @PathVariable String analysis) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("title", title)).should(QueryBuilders.matchQuery("analysis", analysis));
        return subjectDao.query(query);
    }

    @GetMapping("/like_not_title/{title}")
    public List<Subject> likeNotTitle1(@PathVariable String title) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("title", title));
        return subjectDao.query(query);
    }

    @GetMapping("/between1/{start}/{end}")
    public List<Subject> between1(@PathVariable long start, @PathVariable long end) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("id").gte(start).lte(end));
        return subjectDao.query(query);
    }

    @GetMapping("/count_all")
    public Map<String, Object> countAll() throws Exception {
        return ImmutableMap.of("count", subjectDao.count());
    }

    @GetMapping("/group_by")
    public List<Map<String, Object>> groupBy() throws Exception {
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("group_by_grade").field("grade.keyword").subAggregation(AggregationBuilders.avg("average_id").field("id"));
        Terms agg = subjectDao.aggregations(aggregation).get("group_by_grade");
        return agg.getBuckets().stream().collect(ArrayList::new, (l, d) -> {
            Map<String, Object> data = new HashMap<>(2);
            data.put("group_by_grade", d.getKeyAsString());
            Avg avg = d.getAggregations().get("average_id");
            data.put("average_id", avg.getValue());
            l.add(data);
        }, (l1, l2) -> l1.addAll(l2));
    }

    @GetMapping("group_by1")
    public List<Map<String, Object>> groupBy1() throws Exception {
        RangeAggregationBuilder aggregation = AggregationBuilders.range("group_by_groupId").field("groupId")
                .addRange(0, 1000).addRange(1000, 2000).addRange(2000, 3000).addRange(3000, 4000)
                .subAggregation(
                        AggregationBuilders.terms("group_by_grade").field("grade.keyword")
                                .subAggregation(AggregationBuilders.avg("average_id").field("id"))
                );

        ParsedRange agg = subjectDao.aggregations(aggregation).get("group_by_groupId");
        return agg.getBuckets().stream().collect(ArrayList::new, (l, d) -> {
            Map<String, Object> data = new HashMap<>(2);
            data.put("group_by_groupId", d.getKeyAsString());

            Terms groupByGrade = d.getAggregations().get("group_by_grade");
            List<? extends Terms.Bucket> buckets = groupByGrade.getBuckets();
            List<Object> gee = new ArrayList<>();
            for (Terms.Bucket bucket : buckets) {
                Map<String, Object> ndata = new HashMap<>(2);
                ndata.put("group_by_grade", bucket.getKeyAsString());
                Avg avg = bucket.getAggregations().get("average_id");
                ndata.put("average_id", avg.getValue());
                gee.add(ndata);
            }
            data.put("group_by_grades", gee);
            l.add(data);

        }, (l1, l2) -> l1.addAll(l2));
    }

}
