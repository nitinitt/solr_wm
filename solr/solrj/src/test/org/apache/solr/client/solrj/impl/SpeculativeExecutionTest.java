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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpeculativeExecutionTest extends SolrJettyTestBase {

    private static final Logger log = LoggerFactory.getLogger(SpeculativeExecutionTest.class);
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

    public class TestLbClient extends LBHttp2SolrClient {

        public boolean throwExceptionAfterDelay = false;
        public Rsp rsp;
        public int count = 0;

        public TestLbClient(Http2SolrClient httpClient, boolean enableSpeculativeRetry, ExecutorService executorService, String... baseSolrUrls) {
            super(httpClient, enableSpeculativeRetry, executorService, baseSolrUrls);
        }

        @Override
        protected Exception doRequest(String baseUrl, Req req, Rsp rsp, boolean isNonRetryable,
                                      boolean isZombie) throws SolrServerException, IOException {
            count+=1;
            if (throwExceptionAfterDelay) {
                throwExceptionAfterDelay = false;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                throw new SolrServerException("Expected Exception");
            }
            Exception ex = super.doRequest(baseUrl, req, rsp, isNonRetryable, isZombie);
            this.rsp = rsp;
            return ex;
        }
    }

    public void testHappyPathSpeculativeExec() throws IOException, SolrServerException {
        int threadCount = atLeast(2);
        final ExecutorService threads = ExecutorUtil.newMDCAwareFixedThreadPool(threadCount,
                new SolrNamedThreadFactory(getClass().getSimpleName() + "TestScheduler"));

        final String[] url = new String[2];
        url[0] = fooUrl;
        url[1] = barUrl;

        try (Http2SolrClient http2SolrClient = new Http2SolrClient.Builder().build();
             TestLbClient lbClient = new TestLbClient(http2SolrClient, true, threads, url)) {
            deleteDocs(lbClient);
            int testDocCount = 50;
            insertDocs(lbClient, testDocCount);
            QueryResponse resp = queryDocs(lbClient, testDocCount, url);
            Assert.assertEquals("Result should be obtained from Spec Execution", fooUrl, lbClient.rsp.server);
            Assert.assertEquals("Total docs retrieved should be: " + testDocCount/2, testDocCount/2, resp.getResults().size());
        } finally {
            threads.shutdownNow();
        }
    }

    public void testErrorScenarioSpeculativeExecution() throws IOException, SolrServerException {
        int threadCount = atLeast(2);
        final ExecutorService threads = ExecutorUtil.newMDCAwareFixedThreadPool(threadCount,
                new SolrNamedThreadFactory(getClass().getSimpleName() + "TestScheduler"));

        final String[] url = new String[2];
        url[0] = fooUrl;
        url[1] = barUrl;

        try (Http2SolrClient http2SolrClient = new Http2SolrClient.Builder().build();
             TestLbClient lbClient = new TestLbClient(http2SolrClient, true, threads, url)) {
            deleteDocs(lbClient);
            int testDocCount = 50;
            lbClient.throwExceptionAfterDelay = true;
            insertDocs(lbClient, testDocCount);
            QueryResponse resp = queryDocs(lbClient, testDocCount, url);
            Assert.assertEquals("Result should be obtained from Spec Execution", barUrl, lbClient.rsp.server);
            Assert.assertEquals("Total docs retrieved should be: " + testDocCount/2, testDocCount/2, resp.getResults().size());
        } finally {
            threads.shutdownNow();
        }
    }

    private QueryResponse queryDocs(TestLbClient lbClient, int testDocCount, String... url) throws SolrServerException, IOException {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("q", "*:*");
        params.set(CommonParams.ROWS, testDocCount);

        final LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(params), Arrays.asList(url));
        NamedList<Object> result = lbClient.request(req).getResponse();
        QueryResponse resp = new QueryResponse();
        resp.setResponse(result);
        return resp;
    }

    private void insertDocs(TestLbClient lbClient, int testDocCount) throws SolrServerException, IOException {
        for (int i = 0; i < testDocCount; i++) {
            final SolrInputDocument doc = new SolrInputDocument("id", "" + i);
            lbClient.add(doc);
        }

        for (int i=0; i <2; i++) {
            lbClient.commit();
        }
    }

    private void deleteDocs(final LBSolrClient lbSolrClient) throws SolrServerException, IOException {
        lbSolrClient.deleteByQuery("*:*");

        for (int i=0; i<2; i++) {
            lbSolrClient.commit();
        }
    }
}