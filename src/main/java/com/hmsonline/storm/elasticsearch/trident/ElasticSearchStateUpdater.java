//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch.trident;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import com.hmsonline.storm.elasticsearch.mapper.TridentElasticSearchMapper;

public class ElasticSearchStateUpdater extends BaseStateUpdater<ElasticSearchState> {

    /**
     * 
     */
    private static final long serialVersionUID = -3617012135777283898L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchStateUpdater.class);

    private TridentElasticSearchMapper mapper;
    private boolean emitValue;
    private RequestType requestType;

    public ElasticSearchStateUpdater(TridentElasticSearchMapper mapper, RequestType requestType) {
        this(mapper, Boolean.FALSE, requestType);
    }

    public ElasticSearchStateUpdater(TridentElasticSearchMapper mapper, boolean emitValue, RequestType requestType) {
        this.mapper = mapper;
        this.emitValue = emitValue;
        this.requestType = requestType;
    }

    @Override
    public void updateState(ElasticSearchState state, List<TridentTuple> tuples, TridentCollector collector) {
        if(this.requestType == RequestType.INDEX)
            state.createIndices(mapper, tuples, collector);
        else
            state.bulkUpdate(mapper, tuples, collector);
        if (emitValue) {
            for (TridentTuple tuple : tuples) {
                collector.emit(tuple.getValues());
            }
        }
        LOGGER.info("Processed {} tuples.", tuples.size());
    }

    public static enum RequestType {
        INDEX, UPDATE
    }

}
