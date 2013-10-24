//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch.trident;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;
import backtype.storm.topology.FailedException;

import com.hmsonline.storm.elasticsearch.StormElasticSearchConstants;
import com.hmsonline.storm.elasticsearch.StormElasticSearchUtils;
import com.hmsonline.storm.elasticsearch.mapper.TridentElasticSearchMapper;

/**
 * ElasticSearchState can either index or update.
 * By default the state is in StrictMode.  In StrictMode it will fail a transaction batch of tuples if it receives
 * an failures from a response from elasticsearch.  If strictMode is disabled, it will only fail batches if
 * elasticsearch throws an exception while processing a bulk request or if every action returns a failed response.
 */
public class ElasticSearchState implements State {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchState.class);

    private static TransportClient sharedClient;

    private Client client;
    private boolean strictMode;

    @SuppressWarnings("rawtypes")
    public ElasticSearchState(Map config, boolean strictMode) {
        LOGGER.debug("Initialize ElasticSearchState");
        String clusterName = (String) config.get(StormElasticSearchConstants.ES_CLUSTER_NAME);
        String host = (String) config.get(StormElasticSearchConstants.ES_HOST);
        Object portObj = config.get(StormElasticSearchConstants.ES_PORT);
        Integer port = portObj instanceof Long ? ((Long)portObj).intValue() : (Integer)portObj;
        client = getSharedClient(clusterName, host, port);
        LOGGER.debug("Initialization completed with [clusterName=" + clusterName + ", host=" + host + ", port=" + port
                + "]");
        this.strictMode = strictMode;
    }

    public ElasticSearchState(Client client) {
        this.client = client;
        this.strictMode = true;
    }

    @Override
    public void beginCommit(Long txid) {
        LOGGER.debug("Begin to commit ES: " + txid);
    }

    @Override
    public void commit(Long txid) {
        LOGGER.debug("Commit ES: " + txid);
    }

    public void createIndices(TridentElasticSearchMapper mapper, List<TridentTuple> tuples, TridentCollector collector) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Set<String> existingIndex = new HashSet<String>();
        for (TridentTuple tuple : tuples) {
            String indexName = mapper.mapToIndex(tuple);
            String type = mapper.mapToType(tuple);
            String key = mapper.mapToKey(tuple);
            String data = mapper.mapToData(tuple);
            String parentId = mapper.mapToParentId(tuple);

//            if (!existingIndex.contains(indexName)
//                    && !client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
//                createIndex(bulkRequest, indexName, mapper.mapToIndexSettings(tuple));
//                createMapping(bulkRequest, indexName, type, mapper.mapToMappingSettings(tuple));
//                existingIndex.add(indexName);
//            }
            if (StringUtils.isBlank(parentId)) {
                bulkRequest.add(client.prepareIndex(indexName, type, key).setSource(data));
            } else {
                LOGGER.debug("parent: " + parentId);
                bulkRequest.add(client.prepareIndex(indexName, type, key).setSource(data).setParent(parentId));
            }
        }

        try {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                if(strictMode) {
                    // Index failed. Retry!
                    FailedException fail = new FailedException("Cannot create index via ES: " + bulkResponse.buildFailureMessage());
                    collector.reportError(fail);
                    throw fail;
                }
                else {
                    int numRequest = bulkRequest.numberOfActions();
                    int failedCount = 0;
                    for(BulkItemResponse itemResponse : bulkResponse.getItems()) {
                        if(itemResponse.isFailed()) {
                            ++failedCount;
                        }
                    }
                    if(failedCount == numRequest) {
                        FailedException fail = new FailedException("All requests failed: " + bulkResponse.buildFailureMessage());
                        collector.reportError(fail);
                        throw fail;
                    }
                }
            }
        } catch (ElasticSearchException e) {
            StormElasticSearchUtils.handleElasticSearchException(getClass(), e);
        }
    }

    public void bulkUpdate(TridentElasticSearchMapper mapper, List<TridentTuple> tuples, TridentCollector collector) {
        BulkRequestBuilder bulkRequest = this.client.prepareBulk();
        for(TridentTuple tuple : tuples) {
            String indexName = mapper.mapToIndex(tuple);
            String type = mapper.mapToType(tuple);
            String key = mapper.mapToKey(tuple);
            String updateScript = mapper.mapToUpdateScript(tuple);
            Map<String, Object> updateParams = mapper.mapToUpdateScriptParams(tuple);
            bulkRequest.add(this.client.prepareUpdate(indexName, type, key).setScript(updateScript).setScriptParams(updateParams));
        }
        try {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if(bulkResponse.hasFailures()) {
                if(strictMode) {
                    FailedException fail = new FailedException("Cannot update via ES: "+bulkResponse.buildFailureMessage());
                    LOGGER.error("Cannot update via ES: {}",bulkResponse.buildFailureMessage());
                    collector.reportError(fail);
                    throw fail;
                }
                else {
                    int numRequest = bulkRequest.numberOfActions();
                    int failedCount = 0;
                    for(BulkItemResponse itemResponse : bulkResponse.getItems()) {
                        if(itemResponse.isFailed()) {
                            ++failedCount;
                        }
                    }
//                    if(failedCount == numRequest) {
//                        FailedException fail = new FailedException("All requests failed: " + bulkResponse.buildFailureMessage());
//                        collector.reportError(fail);
//                        throw fail;
//                    }
                }
            }
        } catch (ElasticSearchException e ) {
            StormElasticSearchUtils.handleElasticSearchException(getClass(), e);
        }


    }

    private void createIndex(BulkRequestBuilder bulkRequest, String indexName, Settings indicesSettings) {
        try {
            if (indicesSettings != null) {
                client.admin().indices().prepareCreate(indexName).setSettings(indicesSettings).execute().actionGet();
            } else {
                client.admin().indices().prepareCreate(indexName).execute().actionGet();
            }
        } catch (ElasticSearchException e) {
            StormElasticSearchUtils.handleElasticSearchException(getClass(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    private void createMapping(BulkRequestBuilder bulkRequest, String indexName, String indexType, Map json) {
        try {
            if (json != null) {
                client.admin().indices().preparePutMapping(indexName).setType(indexType).setSource(json).execute()
                        .actionGet();
            }
        } catch (ElasticSearchException e) {
            StormElasticSearchUtils.handleElasticSearchException(getClass(), e);
        }
    }

    private static synchronized Client getSharedClient(String clusterName, String host, int port) {
        if(sharedClient != null) {
            return sharedClient;
        }
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        String[] hosts = host.split(",");
        sharedClient = new TransportClient(settings);
        for(String h : hosts) {
            String[] split = h.split(":");
            sharedClient.addTransportAddress(new InetSocketTransportAddress(split[0], port));
        }
        return sharedClient;
    }

}
