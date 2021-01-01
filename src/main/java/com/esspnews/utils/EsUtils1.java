package com.esspnews.utils;

import com.esspnews.dao.Dao;
import com.esspnews.dao.Dao1;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import org.elasticsearch.common.Strings;


import org.elasticsearch.client.IndicesAdminClient;

//import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.client.RestHighLevelClient;

public class EsUtils1 {
	
	public static final  String CLUSTER_NAME="docker-cluster";
	public static final String HOST_IP="127.0.0.1";
	public static final int TCP_PORT=9200;
	
	//宣告靜態變數 不能再方法宣告 否則為不同的方法
	public static RestHighLevelClient client;
	
	
	//設定集群
	static Settings settings=Settings.builder().put("cluster.name",CLUSTER_NAME).build();

	public static RestHighLevelClient getClient() {
         client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost(HOST_IP, TCP_PORT, "http")));
         //System.out.println(client);
        
        return client;
    }
	
	
	//建立索引
    public static void createIndex(String indexName, int shards, int replicas) {  
        // 1、創建創建索引request參數：索引名indexName 
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        
     // 2、設置索引的settings 
        request.settings(Settings.builder().put("index.number_of_shards", shards) //分片數
                .put("index.number_of_replicas", replicas) //副本數
                .build()
        );
        
		try {
			CreateIndexResponse createIndexResponse = client.indices()
			        .create(request, RequestOptions.DEFAULT);
			
			 boolean isIndexCreated = createIndexResponse.isAcknowledged();
		        if (isIndexCreated) {
		            System.out.println("索引" + indexName + "建立成功");
		        } else {
		            System.out.println("索引" + indexName + "建立失敗");
		        }
		        		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
	
	
    public static void setMapping(String indexName, String typeName, String mapping) {
    	
    	//原始
    	PutMappingRequest request = new PutMappingRequest(indexName,typeName,mapping);
    	//System.out.println(request);
    	
    	try {
			AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
			System.out.println(putMappingResponse);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	//GetMappingsResponse getMappingResponse = client.indices().get(request, RequestOptions.DEFAULT);
    	
    	
    	//Map<String, Object> jsonMap = new HashMap<>();
    	
    	//Map<String,Object> map=new HashMap<String, Object>();
    	
    	/*
    	PutMappingRequest request = new PutMappingRequest(indexName).type(typeName).source(mapping, XContentType.JSON);
    	//System.out.println(request);
    	try {
			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
			System.out.println(createIndexResponse);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	*/
    	
    	/*
    	PutMappingRequest request = new PutMappingRequest("test");
    	
    	request.source(
    		    "{\n" +
    		    "  \"properties\": {\n" +
    		    "    \"message\": {\n" +
    		    "      \"type\": \"text\"\n" +
    		    "    }\n" +
    		    "  }\n" +
    		    "}", 
    		    XContentType.JSON);
    	*/
    	
    	/*
    	Map<String, Object> jsonMap = new HashMap<String, Object>();
    	Map<String, Object> message = new HashMap<String, Object>();
    	message.put("type", "text");
    	Map<String, Object> properties = new HashMap<String, Object>();
    	properties.put("message", message);
    	jsonMap.put("properties", properties);
    	request.source(jsonMap); 
    	*/
    	
    	
    	
    	/*
    	try {
			AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
			boolean acknowledged = putMappingResponse.isAcknowledged();
	        System.out.println(acknowledged);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
    	
        /*
    	
    	Map<String, Object> jsonMap = new HashMap<String, Object>();
        Map<String, Object> message = new HashMap<String, Object>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("message", message);
        jsonMap.put("properties", properties);
        request.source(jsonMap);

        AcknowledgedResponse putMappingResponse;
		try {
			putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
			boolean acknowledged = putMappingResponse.isAcknowledged();
	        System.out.println(acknowledged);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        */

    	
    	/*
    	PutMappingRequest request = new PutMappingRequest(indexName);
    	request.source(mapping);
    	System.out.println(request.source(mapping));
    	*/
    	
    	/*
    	
    	PutMappingRequest request = new PutMappingRequest(indexName).type(typeName).source(mapping, XContentType.JSON);
    	System.out.println(request);
    	client.indices().putMapping(putMappingRequest);
    	
    	//boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
    	boolean exists = client.exists(request,RequestOptions.DEFAULT);
        System.out.println(exists);
    	
    	System.out.println(indexName);
    	*/
    	
    	/*
    	try {
			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
			System.out.println(createIndexResponse);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	*/
    	
    	/*
    	PutMappingRequest mapRequest = new PutMappingRequest(indexName);
     	mapRequest.source(mapping, XContentType.JSON);
     	System.out.println(mapping);
     	try {
			AcknowledgedResponse putMappingResponse = client.indices().putMapping(mapRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	*/
    	/*
     	PutMappingRequest request = new PutMappingRequest(indexName);
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        Map<String, Object> message = new HashMap<String, Object>();
        message.put(typeName,indexName);
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("message", message);
        jsonMap.put("properties", properties);
        request.source(jsonMap);

        AcknowledgedResponse putMappingResponse;
		try {
			putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
			boolean acknowledged = putMappingResponse.isAcknowledged();
	        System.out.println(acknowledged);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        */
    	
        /*
    	PutMappingRequest request = new PutMappingRequest(indexName).type(typeName);
    	//System.out.println(putMappingRequestBuilder);
    	
    	
    	try {
    		AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
    		//System.out.println(putMappingRequestBuilder);
    		boolean acknowledged = putMappingResponse.isAcknowledged();
            System.out.println(acknowledged);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
               
    	
    	/*
    	IndicesAdminClient indicesAdminClient = client.admin()
				.indices();
    	
    	CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
    	
    	PutMappingRequestBuilder putMappingRequestBuilder = client.indices().putMapping(request, RequestOptions.DEFAULT)
                .setType(type);
    	
    	IndicesAdminClient indices = client.admin().indices();
    	
    	IndicesAdminClient indices = client.indices();
    	
    	PutMappingRequestBuilder putMappingRequestBuilder = client.indices().preparePutMapping(indexName)
                .setType(type);

        getAdminClient().preparePutMapping(indexName)
                .setType(typeName)
                .setSource(mapping, XContentType.JSON)
                .get();
		*/

        
    }
    
    public static void setMapping1(String indexName, String typeName, String mapping) {
    	/*
    	 CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);

    	 PutMappingRequestBuilder putMappingRequestBuilder = client.indices().preparePutMapping(index)
                 .setType(type);
    	 
    	 PutMappingRequestBuilder builder = new PutMappingRequestBuilder(client.admin().indices(), PutMappingAction.INSTANCE)
    	 
    	 PutMappingRequestBuilder builder = new PutMappingRequestBuilder(client, PutMappingAction.INSTANCE);
    	*/
    	
    	//AdminClient adminClient = client.admin();
    	
    	//IndicesAdminClient indices = client.admin().indices();
    	
    	//IndicesAdminClient indices = client.indices().create(request, RequestOptions.DEFAULT);
    	
    	/*
    	// 执行mapping
        PutMappingRequestBuilder builder = indices.preparePutMapping(indexName).setType(typeName);
    	
        getAdminClient().preparePutMapping(indexName)
                .setType(typeName)
                .setSource(mapping, XContentType.JSON)
                .get();

		*/
        
    }
    
    
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("test");
		
		client=getClient();
		
		//System.out.println(client);
		
		/*
		//1.建立索引 indexName,shards,replicas
		//需要先建立spnews  若重複建立 會執行失敗 無法建立
        EsUtils1.createIndex("spnews50", 3, 0);
		
      //2.設定Mapping
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("id")
                    .field("type", "long")
                    .endObject()
                    .startObject("title")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .field("boost", 2)
                    .endObject()
                    .startObject("key_word")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
                    .startObject("url")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("reply")
                    .field("type", "long")
                    .endObject()
                    .startObject("source")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("postdate")
                    .field("type", "date")
                    .field("format", "yyyy-MM-dd HH:mm:ss")
                    .endObject()
                    .endObject()
                    .endObject();

            String json = Strings.toString(builder);
            System.out.println(json);
            //String json = builder.string();
            //System.out.println(json);
            
            EsUtils1.setMapping("spnews50", "news",json);
        } catch (IOException e) {
            e.printStackTrace();
        }
        */
        
        
        //建立dao1物件
		Dao1 dao1 = new Dao1();
        dao1.getConnection();
        dao1.mysqlToEs();
        //dao1.mysqlTosearchquery();
		
		/*
		//關閉連線
		 if (client != null) {
	           try {
	            	client.close();
	         } catch (IOException e) {
	                e.printStackTrace();
	        }
		 }
		client.close();
		*/
		//10/24進度 elastic search 新增刪除修改 連線elasticsearch
		
		
	}

}
