package com.shahriar.springkafka.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringKafkaConfig {
	
	@Value("${elasticsearch.host}")
	private String elasticSearchHost;
	
	@Value("${elasticsearch.port}")
	private String elasticSearchPort;
	    
	@Bean
	public RestClient createElasticSearchClient() {
		
		RestClient restClient = RestClient.builder(
			    new HttpHost(elasticSearchHost, 9200, "http"),
			    new HttpHost(elasticSearchHost, 9201, "http")).build();
        return restClient;
	}

}
