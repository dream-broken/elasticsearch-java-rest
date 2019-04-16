package com.example.elasticsearchrest.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchInit {

    @Bean
    public RestHighLevelClient getRestClient() {
        return new RestHighLevelClient(RestClient
                .builder(
                        new HttpHost("127.0.0.1", 9111, "http")
                ));
    }
}
