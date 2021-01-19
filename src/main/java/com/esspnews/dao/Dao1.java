package com.esspnews.dao;

import com.esspnews.utils.EsUtils1;



import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//之前測試用
import org.apache.http.HttpHost;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;

//import org.apache.lucene.queryparser.xml.builders.TermQueryBuilder;
//import org.elasticsearch.index.query.TermQueryBuilder;

import org.elasticsearch.index.query.*;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;



public class Dao1 {
	
	private static Connection conn;
	
	public  static void getConnection() {
		
		//設定資料庫連線
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String user="root";
			String password="aaa0936877093";
			String url="jdbc:mysql://localhost:3306/demo?user=root&password=aaa0936877093&useSSL=false";
			//String url="jdbc:mysql://localhost:3306/demo";
			//conn=DriverManager.getConnection(url,user,password);
			conn=DriverManager.getConnection(url);
			
			if(conn!=null) {
				System.out.println("連線成功");
				
			}else {
				
				System.out.println("連線失敗");
			}
			
			
		}catch(ClassNotFoundException e) {
			e.printStackTrace();
		}catch(SQLException e) {
			e.printStackTrace();
		}
		
		
		
	}
	
	
public  static void test() {
		
	IndexRequest request = new IndexRequest(
	        "posts",  // 索引 Index
	        "doc",  // Type 
	        "1");  // 文档 Document Id 
	String jsonString = "{" +
	        "\"user\":\"kimchy\"," +
	        "\"postDate\":\"2013-01-30\"," +
	        "\"message\":\"trying out Elasticsearch\"" +
	        "}";
	//request.source(jsonString, XContentType.JSON); // 文档源格式为 json string
	
	//System.out.println(request.source(jsonString, XContentType.JSON));
	
	//自行建立的 index type id
	GetRequest getRequest = new GetRequest(
	        "posts", 
	        "doc",  
	        "1");  
	
	/*
	String[] includes = new String[]{"message", "*Date"};
	String[] excludes = Strings.EMPTY_ARRAY;
	FetchSourceContext fetchSourceContext =
            new FetchSourceContext(true, includes, excludes);
    //request.fetchSourceContext(fetchSourceContext);
     */
     
	System.out.println(request.source(jsonString, XContentType.JSON));
	//取得client連線
	RestHighLevelClient client=EsUtils1.getClient();
	try {
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		String index = indexResponse.getIndex();
		String type = indexResponse.getType();
        String id = indexResponse.getId();
        //顯示 index type id
		System.out.println(index);
		System.out.println(type);
		System.out.println(id);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	//String message = getResponse.getField("message").getValue();
	
		
		
	}
	
	
	public static void mysqlToEs() {
		
		//新增資料至elasticsearch
		
		//System.out.println("test");
		String sql="select * from news";
		//取得client連線
		RestHighLevelClient client=EsUtils1.getClient();
		
		//TransportClient client=EsUtils.getSingleClient();
		
		
		//讀取sql資料與將資料轉成map形式儲存
		try {
			//讀取sql
            PreparedStatement pstm=conn.prepareStatement(sql);
            System.out.println(pstm);
            //執行sql
            ResultSet resultSet=pstm.executeQuery();
            
            //建立map
            Map<String,Object> map=new HashMap<String, Object>();
          //讀取下一筆資料
            while (resultSet.next()){
            	//讀取id
                int nid=resultSet.getInt(1);
                
              //put 新增map 資料
                map.put("id",nid);
                map.put("title",resultSet.getString(2));
                map.put("key_word",resultSet.getString(3));
                map.put("content",resultSet.getString(4));
                map.put("url",resultSet.getString(5));
                map.put("reply",resultSet.getInt(6));
                map.put("source",resultSet.getString(7));

                String postdatetime=resultSet.getTimestamp(8).toString();

                map.put("postdate",postdatetime.substring(0,postdatetime.length()-2));
			

                System.out.println(map);
               
                
                //IndexRequest 建立 index=spnews/type=news/id=nid 
                IndexRequest request = new IndexRequest("spnews").type("news").id(String.valueOf(nid)); 
                //IndexRequest request = new IndexRequest("test").type("news").id(String.valueOf(nid)); 
                //
                System.out.println(request.source(map));
                
                try {
            		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            		String index = indexResponse.getIndex();
            		String type = indexResponse.getType();
                    String id = indexResponse.getId();
                    //顯示 index type id
            		System.out.println(index);
            		System.out.println(type);
            		System.out.println(id);
            	} catch (IOException e) {
            		// TODO Auto-generated catch block
            		e.printStackTrace();
            	}
                //break;
				
            }
		
           
        } catch (SQLException e) {
            e.printStackTrace();
        }
		
	}
	
	public static void mysqlToEsist() {
			
			
			
			//System.out.println("test");
			String sql="select * from news";
			//取得client連線
			RestHighLevelClient client=EsUtils1.getClient();
			
			//TransportClient client=EsUtils.getSingleClient();
			
			
			
			//讀取sql資料與將資料轉成map形式儲存
			try {
				//讀取sql
	            PreparedStatement pstm=conn.prepareStatement(sql);
	            System.out.println(pstm);
	            //執行sql
	            ResultSet resultSet=pstm.executeQuery();
	            
	            //建立map
	            Map<String,Object> map=new HashMap<String, Object>();
	          //讀取下一筆資料
	            while (resultSet.next()){
	            	//讀取id
	                int nid=resultSet.getInt(1);
	                
	              //put 新增map 資料
	                map.put("id",nid);
	                map.put("title",resultSet.getString(2));
	                map.put("key_word",resultSet.getString(3));
	                map.put("content",resultSet.getString(4));
	                map.put("url",resultSet.getString(5));
	                map.put("reply",resultSet.getInt(6));
	                map.put("source",resultSet.getString(7));
	
	                String postdatetime=resultSet.getTimestamp(8).toString();
	
	                map.put("postdate",postdatetime.substring(0,postdatetime.length()-2));
				
	
	                System.out.println(map);
	               
	                
	                //IndexRequest 建立 index=spnews/type=news/id=nid 
	                IndexRequest request = new IndexRequest("spnews").type("news").id(String.valueOf(nid)); 
	                //
	                System.out.println(request.source("{\"field\":\"value\"}", map));
	                
	              //自行建立的 index type id to search index
	            	GetRequest getRequest = new GetRequest(
	            	        "spnews", 
	            	        "news",  
	            	        "3");  
	                
	            	try {
	            		//whether index exists
						boolean exists = client.exists(getRequest,RequestOptions.DEFAULT);
						 System.out.println(exists);
						 
						 
						 
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            	
	            	
	                break;
					
	            }
			
	           
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
			
		}
	
	
	
	//search 查詢
	public static void mysqlTosearchquery() {
		
		//取得client連線
		RestHighLevelClient client=EsUtils1.getClient();
				
				
		//创建SearchRequest
        SearchRequest searchRequest = new SearchRequest();
        //指定索引为poems
        searchRequest.indices("spnews");
        //创建SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //任意一個字與內容值相符的(查key_word中 有足或協或杯的字)
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("key_word", "足協杯");
        //创建BoolQueryBuilder 用于添加条件
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //完全與內容值相符的(查key_word中=足協杯)
        TermQueryBuilder termQueryBuilder=QueryBuilders.termQuery("id", "1");
        
        //等於sql的like查詢
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("key_word.keyword", "*足協杯*");
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("key_word.keyword", "足協杯");
           
        
        
        //排序 按照索引中的id升序排序
        searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));
        
        
        //取出前幾筆資料 若沒print預測為10筆
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1000);
        
        boolQueryBuilder.must(termQueryBuilder);
                
        //将查询条件放入searchSourceBuilder中
        searchSourceBuilder.query(boolQueryBuilder);
        //searchRequest解析searchSourceBuilder
        searchRequest.source(searchSourceBuilder);
		try {
				//获取SearchResponse
				SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
				//获取分片结果
	            SearchHits hits = searchResponse.getHits();
	            SearchHit[] searchHits = hits.getHits();
	            //获得数据
	            for (SearchHit hit : searchHits) {
	                 String sourceAsString = hit.getSourceAsString();
	                 System.out.println(sourceAsString);            
	                 String index = hit.getIndex();
	                 String type = hit.getType();
	                 String id = hit.getId();
	                 //System.out.println(id);
	                 
	                }
	            
	            System.out.println("共搜索到："+hits.getHits().length+"筆資料");
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
		}
	
	
	//11/7
	
public static void mysqlTosearchquery2() {
		
	
	//System.out.println("test");
			String sql="select * from news where key_word LIKE '足協杯%'";
			//取得client連線
			RestHighLevelClient client=EsUtils1.getClient();
			
			
			
			
			//讀取sql資料與將資料轉成map形式儲存
			try {
				//讀取sql
	            PreparedStatement pstm=conn.prepareStatement(sql);
	            System.out.println(pstm);
	            //執行sql
	            ResultSet resultSet=pstm.executeQuery();
	            
	            //建立map
	            Map<String,Object> map=new HashMap<String, Object>();
	          //讀取下一筆資料
	            while (resultSet.next()){
	            	//讀取id
	                int nid=resultSet.getInt(1);
	                
	              //put 新增map 資料
	                map.put("id",nid);
	                map.put("title",resultSet.getString(2));
	                map.put("key_word",resultSet.getString(3));
	                map.put("content",resultSet.getString(4));
	                map.put("url",resultSet.getString(5));
	                map.put("reply",resultSet.getInt(6));
	                map.put("source",resultSet.getString(7));

	                String postdatetime=resultSet.getTimestamp(8).toString();

	                map.put("postdate",postdatetime.substring(0,postdatetime.length()-2));
				

	                //System.out.println(map);
	                
	              //IndexRequest 建立 index=spnews/type=news/id=nid 
	                IndexRequest request = new IndexRequest("spnews").type("news").id(String.valueOf(nid)); 
	                
	                request.source(map);
	                
	                //System.out.println(request.source(map));
	                
	                //SearchRequest searchRequest = new SearchRequest();
	                //MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("key_word", "足協杯");
	                //BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
	                //boolQueryBuilder.must(matchQueryBuilder);
	                
	                
	              
	                
	                
	                //SearchRequest searchRequest = new SearchRequest();
	                //SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();  // 預設配置
	                //sourceBuilder.query(QueryBuilders.termQuery("key_word", "足協杯"));
	                //searchRequest.source(sourceBuilder);
	                
	                /*
	                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();  // 預設配置
	                sourceBuilder.query(QueryBuilders.termQuery("id", 1)); // 設定搜尋，可以是任何型別的 QueryBuilder
	                sourceBuilder.from(0); // 起始 index
	                sourceBuilder.size(5000); // 大小 size
	                sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); // 設定搜尋的超時時間
	                
	                searchRequest.source(sourceBuilder);
	                */
	                
	                /*
	                try {
						SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
						//System.out.println(response);
						SearchHits hits=response.getHits();
	            		System.out.println("共搜索到："+hits.getHits().length+"筆資料");
	            		
						
						
						SearchHit[] searchHits = hits.getHits();
						//System.out.println(searchHits);
						
						
						for (SearchHit hit : searchHits) {
						    // do something with the SearchHit
							//System.out.println(hit);
							
							//String index = hit.getIndex();
							//String type = hit.getType();
							//String id = hit.getId();
							
							String sourceAsString = hit.getSourceAsString();
			                System.out.println(sourceAsString);
							
							//System.out.println(id);
							
						}
	            		
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                */
	                
	                /*
	                SearchRequest searchRequest = new SearchRequest();
	                searchRequest.indices("spnews");
	                searchRequest.types("news");

	                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	                MatchQueryBuilder match = QueryBuilders.matchQuery("title", "足協杯");
	                match.autoGenerateSynonymsPhraseQuery(false);
	                searchSourceBuilder.query(match);

	                //System.out.println(searchSourceBuilder);
	                searchRequest.source(searchSourceBuilder);
	                try {
						SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
						//System.out.println(response);
						SearchHits hits=response.getHits();
	            		System.out.println("共搜索到："+hits.getTotalHits()+"筆資料");
	            		
						
						
						SearchHit[] searchHits = hits.getHits();
						//System.out.println(searchHits);
						
						
						for (SearchHit hit : searchHits) {
						    // do something with the SearchHit
							System.out.println(hit);
							
							String index = hit.getIndex();
							String type = hit.getType();
							String id = hit.getId();
							
							//System.out.println(id);
							
						}
	            		
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                
	                */
	                
	                /*
	                MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("key_word", "國足");
	            	
	            	//SearchResponse searchResponse = client.search(matchQueryBuilder, RequestOptions.DEFAULT);
	            	
	            	 SearchRequest searchRequest = new SearchRequest("spnews");
	            	 //searchRequest.scroll(scroll);
	            	 SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	            	 searchSourceBuilder.query(matchQueryBuilder);
	            	 searchRequest.source(searchSourceBuilder);
	            	 
	            	try {
	            		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
	            		SearchHits hits=searchResponse.getHits();
	            		System.out.println("共搜索到："+hits.getTotalHits()+"筆資料");
	            		
	            		//String scrollId = searchResponse.getScrollId();
	            		//SearchHit[] searchHits = searchResponse.getHits().getHits();
	            		
	            	} catch (IOException e) {
	            		// TODO Auto-generated catch block
	            		e.printStackTrace();
	            	}
	                
	                
	                /*
	                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	                searchSourceBuilder.query(QueryBuilders.termQuery("key_word", "武漢"));
	                
	                searchSourceBuilder.from(0);
	                searchSourceBuilder.size(100);
	                searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
	                SearchRequest searchRequest2 = new SearchRequest();
	                //index 數據庫
	                searchRequest2.indices("spnews");
	                //searchRequest2.source(searchSourceBuilder);
	                System.out.println(searchRequest2.source(searchSourceBuilder));
	                
	              //同步執行
	                try {
						SearchResponse searchResponse = client.search(searchRequest2, RequestOptions.DEFAULT);
						//Retrieving SearchHits 獲取結果數據
				        SearchHits hits = searchResponse.getHits();
				        System.out.println(hits);
				        
				        
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

	                
	                /*
	                
	                
	                
	              //同步執行
	                try {
						SearchResponse searchResponse = client.search(searchRequest2, RequestOptions.DEFAULT);
						//Retrieving SearchHits 獲取結果數據
				        SearchHits hits = searchResponse.getHits();
				        System.out.println(hits);
				        SearchHit[] searchHits = hits.getHits();
				        
				        for (SearchHit hit : searchHits) {
				            // do something with the SearchHit
				            String index = hit.getIndex();
				            String type = hit.getType();
				            String id = hit.getId();
				            float score = hit.getScore();

				            String sourceAsString = hit.getSourceAsString();
				            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
				            System.out.println(sourceAsString);
				            
				        }
				        
				        
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

	                */
	                
	                /*
	                SearchRequest searchRequest = new SearchRequest();
	                searchRequest.indices("spnews");
	                searchRequest.source(searchSourceBuilder);
	                
					try {
						SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
						  System.out.println("Total: " + response.getHits().toString());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	              
	                */
	                
	                /*
	                
	                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
	                sourceBuilder.query(QueryBuilders.termQuery("key_word", "孔塔"));
	                
	                sourceBuilder.from(0); 
	                sourceBuilder.size(20); 
	                sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); 
	                
	                
	                //System.out.println(sourceBuilder.query(QueryBuilders.termQuery("key_word", "中超")));
	                
	                
	                
	                
	                SearchRequest searchRequest = new SearchRequest();
	                searchRequest.indices("spnews");
	                searchRequest.source(sourceBuilder);
	                //System.out.println(searchRequest.source(sourceBuilder));
	                
	                try {
						SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
						//System.out.println(searchResponse);
						
						SearchHits hits = searchResponse.getHits();
						
						
						SearchHit[] searchHits = hits.getHits();
						//System.out.println(searchHits);
						
						
						for (SearchHit hit : searchHits) {
						    // do something with the SearchHit
							System.out.println(hit);
							
							String index = hit.getIndex();
							String type = hit.getType();
							String id = hit.getId();
							
							System.out.println(id);
							
						}
						
						
						
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                */
	                
	                
	                /*
	               
	                
	                SearchRequest searchRequest = new SearchRequest();
	                searchRequest.indices("spnews");
	                //searchRequest.source(sourceBuilder);
	                System.out.println(searchRequest.source(sourceBuilder));
	                */
	                //break;
	            	
	            	 
	            }
			
	            
	          //创建SearchRequest
                SearchRequest searchRequest = new SearchRequest();
                //指定索引为poems
                searchRequest.indices("spnews");
                //创建SearchSourceBuilder
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
              //创建BoolQueryBuilder 用于添加条件
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                //MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("key_word", "足協杯");
                //matchQueryBuilder.operator(Operator.AND);
                
                //等於sql的like查詢
                WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("key_word.keyword", "*足協杯*");
                
              
                
                //PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("key_word.keyword", "足協杯");
                
                //QueryBuilder queryBuilder = QueryBuilders.termQuery("key_word", "足協杯");
                //org.elasticsearch.index.query.TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("key_word", "足協杯");
                //searchSourceBuilder.query(QueryBuilders.termQuery("key_word", "裡皮,世預賽,國足"));
              //创建BoolQueryBuilder 用于添加条件
               //BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                boolQueryBuilder.must(wildcardQueryBuilder);
                
                //排序 按照索引中的id升序排序
                searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));
                
                
                //取出前幾筆資料 若沒print預測為10筆
                searchSourceBuilder.from(0);
                searchSourceBuilder.size(5570);
                
                
                
                //将查询条件放入searchSourceBuilder中
                searchSourceBuilder.query(boolQueryBuilder);
                //searchRequest解析searchSourceBuilder
                searchRequest.source(searchSourceBuilder);
				try {
					//获取SearchResponse
					SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
					//获取分片结果
	                SearchHits hits = searchResponse.getHits();
	                SearchHit[] searchHits = hits.getHits();
	                //获得数据
	                for (SearchHit hit : searchHits) {
	                    String sourceAsString = hit.getSourceAsString();
	                    System.out.println(sourceAsString);
	                    
	                    
	                    
	                    
	                }
	                System.out.println("共搜索到："+hits.getHits().length+"筆資料");
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	           
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	
	
	
	//取得client連線
	//RestHighLevelClient client=EsUtils1.getClient();
	
	/*
	MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("id", "1");
	
	//SearchResponse searchResponse = client.search(matchQueryBuilder, RequestOptions.DEFAULT);
	
	 SearchRequest searchRequest = new SearchRequest("id");
	 //searchRequest.scroll(scroll);
	 SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	 searchSourceBuilder.query(matchQueryBuilder);
	 searchRequest.source(searchSourceBuilder);
	 
	try {
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHits hits=searchResponse.getHits();
		System.out.println("共搜索到："+hits.getTotalHits()+"筆資料");
		
		//String scrollId = searchResponse.getScrollId();
		//SearchHit[] searchHits = searchResponse.getHits().getHits();
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 
	 */
	
	
	 
	 /*
	 SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
     searchSourceBuilder.query(QueryBuilders.termQuery("key_word", "裡皮,世預賽,國足"));
     
     searchSourceBuilder.from(0);
     searchSourceBuilder.size(5000);
     searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
     SearchRequest searchRequest2 = new SearchRequest();
     //index 數據庫
     searchRequest2.indices("spnews");
     //searchRequest2.source(searchSourceBuilder);
     System.out.println(searchRequest2.source(searchSourceBuilder));
     
   //同步執行
     try {
			SearchResponse searchResponse = client.search(searchRequest2, RequestOptions.DEFAULT);
			//Retrieving SearchHits 獲取結果數據
	        SearchHits hits = searchResponse.getHits();
	        System.out.println(hits);
	        
	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	*/
		
		
		
		
	}
	
	//這邊重研究 11/7
	
	public static void mysqlTosearchquery1() {
		
		
		
		//System.out.println("test");
		String sql="select * from news";
		//取得client連線
		RestHighLevelClient client=EsUtils1.getClient();
		
		
		
		
		//讀取sql資料與將資料轉成map形式儲存
		try {
			//讀取sql
            PreparedStatement pstm=conn.prepareStatement(sql);
            System.out.println(pstm);
            //執行sql
            ResultSet resultSet=pstm.executeQuery();
            
            //建立map
            Map<String,Object> map=new HashMap<String, Object>();
          //讀取下一筆資料
            while (resultSet.next()){
            	//讀取id
                int nid=resultSet.getInt(1);
                
              //put 新增map 資料
                map.put("id",nid);
                map.put("title",resultSet.getString(2));
                map.put("key_word",resultSet.getString(3));
                map.put("content",resultSet.getString(4));
                map.put("url",resultSet.getString(5));
                map.put("reply",resultSet.getInt(6));
                map.put("source",resultSet.getString(7));

                String postdatetime=resultSet.getTimestamp(8).toString();

                map.put("postdate",postdatetime.substring(0,postdatetime.length()-2));
			

                //System.out.println(map);
                
              //IndexRequest 建立 index=spnews/type=news/id=nid 
                IndexRequest request = new IndexRequest("spnews").type("news").id(String.valueOf(nid)); 
                
                //request.source("{\"field\":\"value\"}", map);
                
                System.out.println(request.source("{\"field\":\"value\"}", map));
                
                
                
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.termQuery("key_word", "武漢"));
                
                searchSourceBuilder.from(0);
                searchSourceBuilder.size(100);
                searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
                SearchRequest searchRequest2 = new SearchRequest();
                //index 數據庫
                searchRequest2.indices("spnews");
                //searchRequest2.source(searchSourceBuilder);
                System.out.println(searchRequest2.source(searchSourceBuilder));
                
              //同步執行
                try {
					SearchResponse searchResponse = client.search(searchRequest2, RequestOptions.DEFAULT);
					//Retrieving SearchHits 獲取結果數據
			        SearchHits hits = searchResponse.getHits();
			        System.out.println(hits);
			        
			        
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

                
                /*
                
                
                
              //同步執行
                try {
					SearchResponse searchResponse = client.search(searchRequest2, RequestOptions.DEFAULT);
					//Retrieving SearchHits 獲取結果數據
			        SearchHits hits = searchResponse.getHits();
			        System.out.println(hits);
			        SearchHit[] searchHits = hits.getHits();
			        
			        for (SearchHit hit : searchHits) {
			            // do something with the SearchHit
			            String index = hit.getIndex();
			            String type = hit.getType();
			            String id = hit.getId();
			            float score = hit.getScore();

			            String sourceAsString = hit.getSourceAsString();
			            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			            System.out.println(sourceAsString);
			            
			        }
			        
			        
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

                */
                
                /*
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.indices("spnews");
                searchRequest.source(searchSourceBuilder);
                
				try {
					SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
					  System.out.println("Total: " + response.getHits().toString());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
              
                */
                
                /*
                
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
                sourceBuilder.query(QueryBuilders.termQuery("key_word", "孔塔"));
                
                sourceBuilder.from(0); 
                sourceBuilder.size(20); 
                sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); 
                
                
                //System.out.println(sourceBuilder.query(QueryBuilders.termQuery("key_word", "中超")));
                
                
                
                
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.indices("spnews");
                searchRequest.source(sourceBuilder);
                //System.out.println(searchRequest.source(sourceBuilder));
                
                try {
					SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
					//System.out.println(searchResponse);
					
					SearchHits hits = searchResponse.getHits();
					
					
					SearchHit[] searchHits = hits.getHits();
					//System.out.println(searchHits);
					
					
					for (SearchHit hit : searchHits) {
					    // do something with the SearchHit
						System.out.println(hit);
						
						String index = hit.getIndex();
						String type = hit.getType();
						String id = hit.getId();
						
						System.out.println(id);
						
					}
					
					
					
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                */
                
                
                /*
               
                
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.indices("spnews");
                //searchRequest.source(sourceBuilder);
                System.out.println(searchRequest.source(sourceBuilder));
                */
                //break;
                
				
            }
		
           
        } catch (SQLException e) {
            e.printStackTrace();
        }
		
	}
	
	public static void search() {
		
		/*
		String json = "{" +
		        "\"user\":\"kimchy\"," +
		        "\"postDate\":\"2013-01-30\"," +
		        "\"message\":\"trying out Elasticsearch\"" +
		    "}";
		 */
		
		/*
		String json= "{"+
		    "\"match\":"+"{"+
		      "\"title\":"+"{"+
		        "\"query\":"+"\"足球\""+
		      "}"+
		    "}"+
		  "}"+
		"}";
		*/
		
		//不能加query 不然會有錯誤
		String json = "{\"match\":{\"title\":\"足\"}}";
		
		
		//String dsl = "{\"match\":{\"title\":\"足\"}}";
		
		//Map maps = (Map)JSON.parse(dsl);  
		//maps.get("query");
		
		//取得client連線
		RestHighLevelClient client=EsUtils1.getClient();
						
						
		//创建SearchRequest
		SearchRequest searchRequest = new SearchRequest();
		//指定索引为poems
		searchRequest.indices("spnews4");
		
		//创建SearchSourceBuilder
	    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		
		WrapperQueryBuilder wqbQueryBuilder = QueryBuilders.wrapperQuery(json);
		//将查询条件放入searchSourceBuilder中
	    searchSourceBuilder.query(wqbQueryBuilder);
	    
	  //searchRequest解析searchSourceBuilder
	    searchRequest.source(searchSourceBuilder);
	    
	    try {
	    	
	    	//获取SearchResponse
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			
			//获取分片结果
	        SearchHits hits = searchResponse.getHits();
	        SearchHit[] searchHits = hits.getHits();
	        //获得数据
	        for (SearchHit hit : searchHits) {
	             String sourceAsString = hit.getSourceAsString();
	             System.out.println(sourceAsString);            
	             String index = hit.getIndex();
	             //String type = hit.getType();
	             String id = hit.getId();
	             //System.out.println(id);
	             
	            }
	        
	        System.out.println("共搜索到："+hits.getHits().length+"筆資料");
	    	
	    }catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    
	    
		
		
	}


	public static void analyze() {
		
		//不能加query 不然會有錯誤
		String json = "{\"match\":{\"title\":\"足\"}}";
		
		//取得client連線
		RestHighLevelClient client=EsUtils1.getClient();
		AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
			    "spnews4",         
			    "standard",        
			    "The Quick Brown Fox  "
			);
		
		try {
	    	
	    	//获取analyzeResponse
			AnalyzeResponse analyzeResponse = client.indices().analyze(request, RequestOptions.DEFAULT);
			List<AnalyzeResponse.AnalyzeToken> tokens= analyzeResponse.getTokens();
			for (AnalyzeResponse.AnalyzeToken token : tokens) {
				
				String term = token.getTerm();
				
	            System.out.println(term);
	        }
	    	
	    }catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.print("test");
		//也可使用
		//Dao1.getConnection();
		//getConnection();
		//mysqlToEs();
		//test();
		//mysqlToEsist();
		//mysqlTosearchquery();
	}

}
