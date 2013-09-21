//
// Copyright (c) 2013 Health Market Science, Inc.
//
package com.hmsonline.storm.elasticsearch.trident;

import java.util.Map;

//import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class ElasticSearchStateFactory implements StateFactory {

    /**
     * 
     */
    private static final long serialVersionUID = -6363900741883372326L;

    @Override
    public State makeState(Map conf, int i, int i2) {
        return new ElasticSearchState(conf);
    }

    /*

    Comment out code is for version 0.8.2

    @SuppressWarnings("rawtypes")
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new ElasticSearchState(conf);
    }
    */
}
