package com.esspnews.controller;

//import com.esspnews.utils.EsUtils;
import com.esspnews.utils.EsUtils1;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.search.MultiMatchQuery;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

//建立url
@WebServlet(name = "/SearchNews", urlPatterns = "/SearchNews")
public class SearchServlet1 extends HttpServlet{
	
	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		//設定中文編碼格式
        req.setCharacterEncoding("UTF-8");
        
        //取得參數
        String query = req.getParameter("query");
        System.out.println(query);
        
        //http://localhost:8080/esspnews/SearchNews?query=cba&pageNum=4
        //顯示 pageNum 頁數
        String pageNumStr=req.getParameter("pageNum");
        System.out.println(pageNumStr);
        int pageNum=1;

        if (pageNumStr!=null&&Integer.parseInt(pageNumStr)>1){
            pageNum=Integer.parseInt(pageNumStr);
        }
        searchSpnews(query, pageNum,req);
        //setAttribute 設定屬性
        req.setAttribute("queryBack", query);
        req.getRequestDispatcher("result.jsp").forward(req, resp);
        

    }
	
	
	private void searchSpnews(String query, int pageNum,HttpServletRequest req) {

        long start = System.currentTimeMillis();
        //TransportClient client = EsUtils.getSingleClient();
      //取得client連線
		RestHighLevelClient client=EsUtils1.getClient();
        /*
		MultiMatchQueryBuilder multiMatchQuery = QueryBuilders
                .multiMatchQuery(query, "title", "content");
                */
		//顯示關鍵字搜尋出來的結果(新聞標題) 標紅色
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .preTags("<span style=\"color:red\">")
                .postTags("</span>")
                .field("title")
                .field("content");
        
      //创建SearchRequest
        SearchRequest searchRequest = new SearchRequest();
        //指定索引为poems
        searchRequest.indices("spnews");
        //searchRequest.searchType("news");
      //创建SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        //設定搜尋結果 "title", "content" 資料庫欄位
        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders
                .multiMatchQuery(query, "title", "content");
        //MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("key_word", "足協杯");
        //创建BoolQueryBuilder 用于添加条件
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        
        
        //排序 按照索引中的id升序排序
        searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));
        
        
        //取出前幾筆資料 若沒print預測為10筆
        searchSourceBuilder.from((pageNum-1)*5);
        //size 設定資料查詢結果筆數
        searchSourceBuilder.size(5);
        
        boolQueryBuilder.must(multiMatchQuery);
                
        //将查询条件放入searchSourceBuilder中
        searchSourceBuilder.query(boolQueryBuilder);
        //searchRequest解析searchSourceBuilder
        searchRequest.source(searchSourceBuilder);
        
        searchSourceBuilder.highlighter(highlightBuilder);
        
      //获取SearchResponse 顯示查詢結果
		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			//获取分片结果
            SearchHits hits = searchResponse.getHits();
            
            //將結果儲存至array
            ArrayList<Map<String, Object>> newslist = new ArrayList<Map<String, Object>>();
            for (SearchHit hit : hits) {
                Map<String, Object> news = hit.getSourceAsMap();
                
                //取得特定標記的欄位
                HighlightField hTitle = hit.getHighlightFields().get("title");
                if (hTitle != null) {
                    Text[] fragments = hTitle.fragments();
                    String hTitleStr = "";
                    for (Text text : fragments) {
                        hTitleStr += text;
                    }
                    news.put("title", hTitleStr);
                }

                HighlightField hContent = hit.getHighlightFields().get("content");
                if (hContent != null) {
                    Text[] fragments = hContent.fragments();
                    String hContentStr = "";
                    for (Text text : fragments) {
                        hContentStr += text;
                    }
                    news.put("content", hContentStr);
                }
                newslist.add(news);
            }
            long end = System.currentTimeMillis();
            //顯示搜尋到的結果時間筆數
            req.setAttribute("newslist", newslist);
            req.setAttribute("totalHits", hits.getTotalHits() + "");
            req.setAttribute("totalTime", (end - start) + "");
            
            
				
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		
        
    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
