/**
 * 
 */
package com.hmsonline.storm.elasticsearch.mapper;

import java.io.Serializable;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;

import storm.trident.tuple.TridentTuple;

/**
 * @author irieksts
 * 
 */
public interface TridentElasticSearchMapper extends Serializable {
    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map to the index
     * name.
     * 
     * @param tuple
     * @return
     */
    public String mapToIndex(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map to the index
     * type.
     * 
     * @param tuple
     * @return
     */
    public String mapToType(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map to the index
     * key.
     * 
     * @param tuple
     * @return
     */
    public String mapToKey(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map to the parent
     * ID. If null or blank will ignore.
     * 
     * @param tuple
     * @return
     */
    public String mapToParentId(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map to the index
     * data.
     * 
     * @param tuple
     * @return json string
     */
    public String mapToData(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map to the update script.
     * @param tuple
     * @return  text of a valid elasticsearch update script.
     */
    public String mapToUpdateScript(TridentTuple tuple);

    /**
     * Given a <code>backtype.storm.tuple.Tuple</code> object, map to the update script parameters.
     * @param tuple
     * @return parameters for update script.
     */
    public Map<String, Object> mapToUpdateScriptParams(TridentTuple tuple);


    public Settings mapToIndexSettings(TridentTuple tuple);

    @SuppressWarnings("rawtypes")
    public Map mapToMappingSettings(TridentTuple tuple);
}
