package com.example.elasticsearchrest.controller;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.*;
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
@RequestMapping
public class ESController {

    @Resource
    private RestHighLevelClient restHighLevelClient;

    @GetMapping("/all")
    public List<Map<String, Object>> all() throws Exception {
        SearchResponse searchResponse = restHighLevelClient.search(new SearchRequest("build"), RequestOptions.DEFAULT);
        return this.analysis(searchResponse);
    }

    @GetMapping("/all_page/{num}/{size}")
    public List<Map<String, Object>> allPage(@PathVariable int num, @PathVariable int size) throws Exception {
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().from(num).size(size));
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        return this.analysis(searchResponse);
    }

    @GetMapping("/all_page_sort/{num}/{size}/{sort}")
    public List<Map<String, Object>> allPageSort(@PathVariable int num, @PathVariable int size, @PathVariable String sort) throws Exception{
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().from(num).size(size).sort(sort));
        return this.analysis(restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT));
    }

    @GetMapping("/match/{title}")
    public List<Map<String, Object>> match(@PathVariable String title) throws Exception {
        MatchQueryBuilder query = QueryBuilders.matchQuery("title", title);
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().query(query).from(0).size(5));
        return this.analysis(restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT));
    }

    @GetMapping("/match_phrase/{title}")
    public List<Map<String, Object>> matchPhrase(@PathVariable String title) throws Exception {
        MatchPhraseQueryBuilder query = QueryBuilders.matchPhraseQuery("title", title);
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().query(query).from(0).size(5));
        return this.analysis(restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT));
    }

    @GetMapping("/like_title_and_analysis/{title}/{analysis}")
    public List<Map<String, Object>> likeTitleAndAnalysis1(@PathVariable String title, @PathVariable String analysis) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", title)).must(QueryBuilders.matchQuery("analysis", analysis));
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().query(query).from(0).size(5));
        return this.analysis(restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT));
    }

    @GetMapping("/like_title_or_analysis/{title}/{analysis}")
    public List<Map<String, Object>> likeTitleOrAnalysis1(@PathVariable String title, @PathVariable String analysis) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("title", title)).should(QueryBuilders.matchQuery("analysis", analysis));
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().query(query).from(0).size(5));
        return this.analysis(restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT));
    }

    @GetMapping("/like_not_title/{title}")
    public List<Map<String, Object>> likeNotTitle1(@PathVariable String title) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("title", title));
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().query(query).from(0).size(5));
        return this.analysis(restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT));
    }

    @GetMapping("/between1/{start}/{end}")
    public List<Map<String, Object>> between1(@PathVariable long start, @PathVariable long end) throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("id").gte(start).lte(end));
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().query(query).from(0).size(5));
        return this.analysis(restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT));
    }

    @GetMapping("/count_all")
    public Map<String, Object> countAll() throws Exception {
        CountResponse count = restHighLevelClient.count(new CountRequest("build"), RequestOptions.DEFAULT);
        return ImmutableMap.of("count", count.getCount());
    }

    @GetMapping("/group_by")
    public List<Map<String, Object>> groupBy() throws Exception {
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("group_by_grade").field("grade.keyword").subAggregation(AggregationBuilders.avg("average_id").field("id"));
        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().aggregation(aggregation).from(0).size(5));
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Terms agg = searchResponse.getAggregations().get("group_by_grade");
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

        SearchRequest searchRequest = new SearchRequest("build").source(new SearchSourceBuilder().aggregation(aggregation).from(0).size(5));
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ParsedRange agg = searchResponse.getAggregations().get("group_by_groupId");

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

    private List<Map<String, Object>> analysis(SearchResponse searchResponse) {
        List<Map<String, Object>> data = new ArrayList<>();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            data.add(hit.getSourceAsMap());
        }
        return data;
    }

}
