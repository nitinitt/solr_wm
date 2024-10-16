/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HttpSolrClientConPoolTest extends SolrJettyTestBase {

  protected static JettySolrRunner yetty;
  private static String fooUrl;
  private static String barUrl;
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
    // stealing the first made jetty
    yetty = jetty;
    barUrl = yetty.getBaseUrl().toString() + "/" + "collection1";
    
    createAndStartJetty(legacyExampleCollection1SolrHome());
    
    fooUrl = jetty.getBaseUrl().toString() + "/" + "collection1";
  }
  
  @AfterClass
  public static void stopYetty() throws Exception {
    if (null != yetty) {
      yetty.stop();
      yetty = null;
    }
  }

  public class SlowLbClient extends LBHttp2SolrClient {

      private int i = 0;

      public SlowLbClient(Http2SolrClient httpClient, boolean enableSpeculativeRetry, ExecutorService executorService, String... baseSolrUrls) {
          super(httpClient, enableSpeculativeRetry, executorService, baseSolrUrls);
      }

      @Override
      protected Exception doRequest(String baseUrl, Req req, Rsp rsp, boolean isNonRetryable,
                                    boolean isZombie) throws SolrServerException, IOException {
          System.out.println("=========in do request===========" + i);
          if (i == 0) {
              i+=1;
              try {
                  Thread.sleep(2000);
              } catch (InterruptedException e) {
                  throw new RuntimeException(e);
              }
              throw new SolrServerException("Expected Exception");
          }
          return super.doRequest(baseUrl, req, rsp, isNonRetryable, isZombie);
      }
  }

  public void testLBClient() throws IOException, SolrServerException {
     int threadCount = atLeast(2);
      final ExecutorService threads = ExecutorUtil.newMDCAwareFixedThreadPool(threadCount,
              new SolrNamedThreadFactory(getClass().getSimpleName()+"TestScheduler"));

      Http2SolrClient http2SolrClient = new Http2SolrClient.Builder().connectionTimeout(1000).idleTimeout(2000).build();
      final String[] url = new String[2];
      url[0] = fooUrl;
      url[1] = barUrl;

      SlowLbClient roundRobin = null;
      System.out.println("Foo:" + fooUrl + ", bar: " + barUrl);
      try{
        roundRobin = new SlowLbClient(http2SolrClient, true, threads, url);
        roundRobin.deleteByQuery("*:*");
        roundRobin.commit();

        for (int i=0; i<10; i++) {
          final SolrInputDocument doc = new SolrInputDocument("id", ""+i);
          roundRobin.add(doc);
        }
        roundRobin.commit();

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("q", "*:*");
        final LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(params), Arrays.asList(url));
        NamedList<Object> result = roundRobin.request(req).getResponse();
        result.forEach(res -> {
          System.out.println("NcRes: " + res.getKey() + ", Value: " + res.getValue());
        });
      } finally {
        http2SolrClient.close();
        roundRobin.close();
        threads.shutdownNow();
      }
    }

}
