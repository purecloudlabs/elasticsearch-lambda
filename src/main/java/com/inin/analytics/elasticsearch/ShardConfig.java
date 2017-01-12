package com.inin.analytics.elasticsearch;

import java.util.HashMap;
import java.util.Map;

/**
 * While many ES clusters have a uniform number of shards per index, if your
 * indexing volume ebbs & flows it makes sense to vary the shard count from day
 * to day. Given that you're rebuilding indices for historical data one usage pattern
 * has a script reach out to the ES cluster to retrieve the size of each index so
 * a shard count can be be calculated.
 * 
 * For example, perhaps a desirable shard size is 10GB/shard. If the index on the cluster
 * currently takes 25GB, it could be calculated that 3 shards for said index would be appropriate.
 * 
 * Note: Shard size metrics on live ES clusters aren't %100 accurate metric b/c it can vary during merge
 * operations, but the technique is still useful nonetheless. Historical data tends to be less mutable
 * leading to fewer merges. It's up to the implementer to determine an algorithm that fits their
 * implementation. 
 * 
 * 
 * @author drew
 *
 */
public class ShardConfig {
    private Map<String, Long> shardsPerIndex = new HashMap<>();
    private Map<String, Long> shardsPerOrg = new HashMap<>();
    private Long defaultShardsPerIndex = 5l;
    private Long defaultShardsPerOrg = 2l;
    
    public ShardConfig(Map<String, Long> shardsPerIndex, Map<String, Long> shardsPerOrg) {
        super();
        this.shardsPerIndex = shardsPerIndex;
        this.shardsPerOrg = shardsPerOrg;
    }
        
    public ShardConfig(Map<String, Long> shardsPerIndex, Map<String, Long> shardsPerOrg, Long defaultShardsPerIndex, Long defaultShardsPerOrg) {
        super();
        this.shardsPerIndex = shardsPerIndex;
        this.shardsPerOrg = shardsPerOrg;
        this.defaultShardsPerIndex = defaultShardsPerIndex;
        this.defaultShardsPerOrg = defaultShardsPerOrg;
    }
    
    public ShardConfig() {
        super();
    }
    
    public ShardConfig(Long defaultShardsPerIndex, Long defaultShardsPerOrg) {
        super();
        this.defaultShardsPerIndex = defaultShardsPerIndex;
        this.defaultShardsPerOrg = defaultShardsPerOrg;
    }

    public Long getShardsForIndex(String index) {
        if(shardsPerIndex.containsKey(index)) {
            return shardsPerIndex.get(index);
        } 
        
        return defaultShardsPerIndex;
    }
    public Long getShardsForOrg(String index) {
        if(shardsPerOrg.containsKey(index)) {
            return shardsPerOrg.get(index);
        }
        return defaultShardsPerOrg;
    }
    
    

    @Override
    public String toString() {
        return "ShardConfig [shardsPerIndex=" + shardsPerIndex + ", shardsPerOrg=" + shardsPerOrg
                + ", defaultShardsPerIndex=" + defaultShardsPerIndex + ", defaultShardsPerOrg=" + defaultShardsPerOrg
                + "]";
    }

}
