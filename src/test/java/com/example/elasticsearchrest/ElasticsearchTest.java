package com.example.elasticsearchrest;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;

public class ElasticsearchTest {

    private RestHighLevelClient client;

    @Before
    public void initClient() {
        client = new RestHighLevelClient(RestClient
                .builder(
                        new HttpHost("127.0.0.1", 9111, "http")
                ));
    }

    @Test
    public void add() throws Exception {
        IndexRequest request = new IndexRequest();
        //Index
        request.index("posts");
        //id
        request.id("2");

        //1.以字符串形式提供的文档源
        /*String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);*/

        //2.以映射形式提供的文档源，该映射将自动转换为JSON格式
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        request.source(jsonMap);

        //3.文档源作为XContentBuilder对象提供，Elasticsearch内置助手生成JSON内容
        /*XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        request.source(builder);*/

        //4.作为对象键对提供的文档源，它被转换为JSON格式
        /*request.source("user", "kimchy",
                "postDate", new Date(),
                "message", "trying out Elasticsearch");*/

        //路由值
        //request.routing("routing");

        //设置超时（Timeout to wait for primary shard to become available）
        /*request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");*/

        //设置更新策略
        /*request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");*/

        //设置版本
        //request.version(2);

        //设置版本类型
        //request.versionType(VersionType.EXTERNAL);

        //执行类型
        /*request.opType(DocWriteRequest.OpType.CREATE);
        request.opType("create");*/

        //管道
        //request.setPipeline("pipeline");

        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        System.err.println(index);
    }

    @Test
    public void addAsync() {
        IndexRequest request = new IndexRequest("posts").id("1").source("user", "kimchy","postDate", new Date(),"message", "trying out Elasticsearch");

        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            //当执行成功完成时调用。
            @Override
            public void onResponse(IndexResponse indexResponse) {

            }
            //当整个索引请求失败时调用。
            @Override
            public void onFailure(Exception e) {

            }
        };

        client.indexAsync(request, RequestOptions.DEFAULT, listener);
    }

    @Test
    public void get() throws Exception {
        GetRequest request = new GetRequest("posts","1");

        //不显示_source
        //getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        //只显示message和以Date结尾的的字段
        //getRequest.fetchSourceContext(new FetchSourceContext(true, new String[]{"message", "*Date"}, Strings.EMPTY_ARRAY));

        //倾向
        //request.preference("message");

        //实时
        //request.realtime(false);

        //刷新
        //request.refresh(true);

        //版本
        //request.version(2);


        GetResponse documentFields = client.get(request, RequestOptions.DEFAULT);
        if (documentFields.isExists()) {
            System.err.println(documentFields);
            String sourceAsString = documentFields.getSourceAsString();
            System.err.println(sourceAsString);
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            System.err.println(sourceAsMap);
            byte[] sourceAsBytes = documentFields.getSourceAsBytes();
            System.err.println(sourceAsBytes);
        }

        //捕获异常
        try {
            GetResponse getResponse = client.get(new GetRequest("does_not_exist", "1"), RequestOptions.DEFAULT);
            System.err.println(getResponse);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void exists() throws Exception {
        boolean exists = client.exists(new GetRequest("posts", "2").fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE), RequestOptions.DEFAULT);
        System.err.println(exists);
    }

    @Test
    public void delete() throws Exception {
        DeleteRequest request = new DeleteRequest("posts","1");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.err.println(delete);
    }

    @Test
    public void update() throws Exception {
        UpdateRequest request = new UpdateRequest("posts","1");

        //脚本更新
        Map<String, Object> parameters = singletonMap("add", "上海");
        /*Script inline = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,"ctx._source.user = params.user", parameters);
        request.script(inline);*/

        //直接更新
        request.doc("user", "dream", "age", 241);

        //设置由于文档在获取和更新之间被更新而发生版本冲突的重试次数。默认值为0。
        request.retryOnConflict(3);

        request.detectNoop(false);

        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.err.println(update);
    }

    @Test
    public void bulk() throws Exception {
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("posts").id("3")
                .source(XContentType.JSON,"field", "foo"));
        request.add(new IndexRequest("posts").id("4")
                .source(XContentType.JSON,"field", "bar"));
        request.add(new IndexRequest("posts").id("5")
                .source(XContentType.JSON,"field", "baz"));

        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
        System.err.println(bulk);
    }

    @Test
    public void search1() throws Exception {
        SearchRequest searchRequest = new SearchRequest("build");
        /*SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);*/


        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        System.err.println(search);
    }

    @Test
    public void search2() throws Exception {
        SearchRequest searchRequest = new SearchRequest("posts");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        sourceBuilder.from(0);
        sourceBuilder.size(2);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "kimchy")
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);

        searchRequest.source(sourceBuilder);

        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        System.err.println(search);
        System.out.println(search.getHits());
        for (SearchHit hit : search.getHits()) {
            System.out.println(hit);
            System.out.println(hit.getSourceAsMap());
        }
    }

    @Test
    public void count() throws Exception{
        CountRequest countRequest = new CountRequest("posts");
        CountResponse count = client.count(countRequest, RequestOptions.DEFAULT);
        System.err.println(count);

    }
}
