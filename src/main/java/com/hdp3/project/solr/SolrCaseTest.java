package com.hdp3.project.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * @author liuyzh
 * @description: 远程连接Solr客户端并对document执行增删改查操作
  @date 2019/08/02 9:08
 */
public class SolrCaseTest {
    private static final Logger logger = LoggerFactory.getLogger(SolrCaseTest.class);

    private static final List<String> SOLR_URLS = Arrays.asList("http://node71.xdata:8886/solr/");
    private static final String COLLECTION = "collection1";
    private static final int SOKET_TIMEOUT = 20000;
    private static final int ZK_CONNECT_TIMEOUT = 30000;
    private CloudSolrClient cloudSolrClient;

    /**
     * @description: 连接cloudSolr client端
     * @author: liuyzh
     * @date: 2019/08/02
     * @return: void
     */
    @Test
    @Before
    public void solrConnect() {

        // 第一种方式：连接运行中的某一Solr节点
//        String solrUrl = "http://node71.xdata:8886/solr/" + COLLECTION;
//        // 创建solrClient同时指定超时时间，不指定就为默认配置
//        solrClient = new HttpSolrClient.Builder(solrUrl)
//                .withConnectionTimeout(10000)
//                .withSocketTimeout(60000)
//                .build();

        // 第二种方式（推荐），初始化CloudSolrClient。
        // 参考网址：https://lucene.apache.org/solr/7_4_0/solr-solrj/org/apache/solr/client/solrj/impl/CloudSolrClient.Builder.html#Builder-java.util.List-
        // 初始化CloudSolrClient
        cloudSolrClient = new CloudSolrClient.Builder(SOLR_URLS).withSocketTimeout(SOKET_TIMEOUT).withConnectionTimeout(ZK_CONNECT_TIMEOUT).build();
        // 设置一个collection
        cloudSolrClient.setDefaultCollection(COLLECTION);
    }


    /**
     * @description: 新增或更新文档，文档id的值若存在则更新，不存在则新增
     * @author: liuyzh
     * @date: 2019/08/02
     * @return: void
     */
    @Test
    public void aouDocument() {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "doc05");
        document.addField("item_title", "华为手机");
        document.addField("item_price", 4999);
        document.addField("item_info", "限时折扣，速速抢购");
        try {
            // 把文档写入索引库中
            cloudSolrClient.add(document);
            // 提交
            cloudSolrClient.commit();
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * @description: 批量增加文档
     * @author: liuyzh
     * @date: 2019/08/02
     */
    @Test
    public void batchAddDocument() {
        Collection<SolrInputDocument> docs = new ArrayList<>();
        int sum = 6;
        for (int i = 1; i < sum; i++) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            solrInputDocument.addField("id", "doc0" + i);
            solrInputDocument.addField("solr_num", i);
            docs.add(solrInputDocument);
        }
        try {
            cloudSolrClient.add(docs);
            cloudSolrClient.commit();
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * @description: 查询数据：采用id倒序排序，添加过滤查询，并设置分页参数
     * @author: liuyzh
     * @date: 2019/08/02
     */
    @Test
    public void queryDocument() {
        SolrQuery solrQuery = new SolrQuery();
        // 设置查询条件
        solrQuery.set("q", "*:*");  // 查询所有
        solrQuery.addSort("id", SolrQuery.ORDER.desc);  // 采用id倒序排序
        solrQuery.addFilterQuery("id:[doc01 TO doc06]");  // 过滤查询
        // 设置分页参数
        solrQuery.setStart(0);  // 从第几页开始
        solrQuery.setRows(10);  // 每一页展示多少条
        try {
            // 查询
            QueryResponse query = cloudSolrClient.query(solrQuery);
            // 获得查询结果
            SolrDocumentList results = query.getResults();
            // 获得查询记录数
            long numFound = results.getNumFound();
            System.out.println(numFound);
            for (SolrDocument sd : results) {
                System.out.println(sd);
                String id = (String) sd.getFieldValue("id");
                String itemTitle = sd.get("item_title").toString();
                String itemPrice = sd.get("item_price").toString();
                System.out.println("id:" + id);
                System.out.println("itemTitle:" + itemTitle);
                System.out.println("itemPrice:" + itemPrice);
            }
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * @description: 根据查询条件删除文档
     * @author: liuyzh
     * @date: 2019/08/02
     * @return: void
     */
    @Test
    public void deleteDocumentByQuery() {
        try {
            cloudSolrClient.deleteByQuery("id:doc01");
            cloudSolrClient.commit();
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * @description: 使用唯一id删除文档
     * @author: liuyzh
     * @date: 2019/08/02
     * @return: void
     */
    @Test
    public void deleteDocumentById() {
        try {
            cloudSolrClient.deleteById("doc02");
            cloudSolrClient.commit();
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * @description: 关闭cloudSolr client端连接
     * @author: liuyzh
     * @date: 2019/08/02
     * @return: void
     */
    @Test
    @After
    public void closeConnect() {
        if (cloudSolrClient != null) {
            try {
                cloudSolrClient.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }


}
