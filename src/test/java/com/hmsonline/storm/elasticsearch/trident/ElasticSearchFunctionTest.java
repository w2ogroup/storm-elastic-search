//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.elasticsearch.StormElasticSearchAbstractTest;
import com.hmsonline.storm.elasticsearch.mapper.TridentElasticSearchMapper;
import static com.hmsonline.storm.elasticsearch.trident.ElasticSearchStateUpdater.*;


public class ElasticSearchFunctionTest extends StormElasticSearchAbstractTest {

    private String indexName = "index_name";
    private String indexType = "index_type";
    private String indexKey = "index_key";
    private String updateScript = "ctx._source.message = messParam";

    @Test
    public void testFunction() {
        ElasticSearchStateUpdater function = new ElasticSearchStateUpdater(createMapper(), RequestType.INDEX);
        ElasticSearchState state = new ElasticSearchState(getClient());
        function.updateState(state, getTuples(), Mockito.mock(TridentCollector.class));

        GetResponse response = getClient().prepareGet(indexName, indexType, indexKey).execute().actionGet();
        Assert.assertEquals(indexName, response.getIndex());
        Assert.assertEquals(indexType, response.getType());
        Assert.assertEquals(indexKey, response.getId());

        Assert.assertEquals("kimchy", response.getSource().get("user"));
        Assert.assertEquals("trying out Elastic Search", response.getSource().get("message"));

        function = new ElasticSearchStateUpdater(createMapper(), RequestType.UPDATE);
        function.updateState(state, getTuples(), Mockito.mock(TridentCollector.class));

        response = getClient().prepareGet(indexName, indexType, indexKey).execute().actionGet();
        Assert.assertEquals(indexName, response.getIndex());
        Assert.assertEquals(indexType, response.getType());
        Assert.assertEquals(indexKey, response.getId());

        Assert.assertEquals("kimchy", response.getSource().get("user"));
        Assert.assertEquals("trying out Elastic Search update", response.getSource().get("message"));


    }

    private TridentElasticSearchMapper createMapper() {
        TridentElasticSearchMapper mapper = Mockito.mock(TridentElasticSearchMapper.class);
        Mockito.when(mapper.mapToIndex(Mockito.any(TridentTuple.class))).thenReturn(indexName);
        Mockito.when(mapper.mapToType(Mockito.any(TridentTuple.class))).thenReturn(indexType);
        Mockito.when(mapper.mapToKey(Mockito.any(TridentTuple.class))).thenReturn(indexKey);

        Map<String, Object> json = new HashMap<String, Object>();
//        json.put("user", "kimchy");
        json.put("messParam", "trying out Elastic Search update");
        Mockito.when(mapper.mapToData(Mockito.any(TridentTuple.class))).thenReturn("{\"user\":\"kimchy\",\"message\":\"trying out Elastic Search\"}");
        Mockito.when(mapper.mapToParentId(Mockito.any(TridentTuple.class))).thenReturn(null);
        Mockito.when(mapper.mapToIndexSettings(Mockito.any(TridentTuple.class))).thenReturn(null);
        Mockito.when(mapper.mapToMappingSettings(Mockito.any(TridentTuple.class))).thenReturn(null);
        Mockito.when(mapper.mapToUpdateScript(Mockito.any(TridentTuple.class))).thenReturn(updateScript);
        Mockito.when(mapper.mapToUpdateScriptParams(Mockito.any(TridentTuple.class))).thenReturn(json);
        return mapper;
    }

    private List<TridentTuple> getTuples() {
        List<TridentTuple> tuples = new ArrayList<TridentTuple>();
        tuples.add(Mockito.mock(TridentTuple.class));
        return tuples;
    }
}
