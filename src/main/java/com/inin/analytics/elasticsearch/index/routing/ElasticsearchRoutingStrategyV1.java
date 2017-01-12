package com.inin.analytics.elasticsearch.index.routing;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.inin.analytics.elasticsearch.index.rotation.ElasticSearchIndexMetadata;
import com.inin.analytics.elasticsearch.util.MurmurHash;


/**
 * This routing strategy for elasticsearch. Read up on what routing does here
 * http://www.elasticsearch.org/blog/customizing-your-document-routing/ 
 * 
 * Perhaps you have 10 shards per index and you don't wish to query every shard 
 * every time you do a search against an org. A simple sharding strategy would
 * put all the data for 1 org on 1 shard using consistant hashing on orgId. However that 
 * has the potential to hotspot some shards if an org with a lot of data comes
 * through.
 * 
 * This attempts to alleviate that by making the # subset of shards configurable. EG
 * numShards = 10, numShardsPerOrg = 3, all of an org's data will be split to one of 
 * 3 shards. Which one of the 3 is determined by hashing the conversationId.
 *  
 * Note: DO NOT CHANGE THIS CLASS. It's immutable once it's been used to generate ES indexes
 * so changing it affects data routing and will make data appear unavailable b/c its looking
 * in the wrong shard. The correct thing to do is to make a newer version of this class,
 * say ElasticsearchRoutingStrategyV2 and see to it that the hadoop jobs to rebuild
 * the ES indexes not only use it, but update zookeeper with which implementation
 * indexes were built with. That way you can evolve the routing strategy without breaking
 * anything.
 * 
 * @author drew
 *
 */
public class ElasticsearchRoutingStrategyV1 implements ElasticsearchRoutingStrategy, java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private int numShardsPerOrg;
	private int numShards;
	private Map<Integer, Integer> shardToRout = new HashMap<>();

	/**
	 * Adapted from DjbHashFunction & PlainOperationRouting in Elasticsearch. This is the default hashing 
	 * Algorithm for doc routing. We need this to reverse engineer routing strings that rout to
	 * the shard we want. 
	 * 
	 * @param value
	 * @return
	 */
	public int hash(String value) {
		long hash = 5381;
		for (int i = 0; i < value.length(); i++) {
			hash = ((hash << 5) + hash) + value.charAt(i);
		}

		return Math.abs((int) hash % numShards);
	}

	public void init() {
		Integer x = 0;
		while(shardToRout.size() < numShards) {
			Integer hash = hash(x.toString());
			if(shardToRout.get(x) == null) {
				shardToRout.put(x, hash);
			}
			x++;
		}
	}

	public ElasticsearchRoutingStrategyV1() {

	}

	@Override
	public void configure(ElasticSearchIndexMetadata rotatedIndexMetadata) {
		Preconditions.checkNotNull(rotatedIndexMetadata.getNumShardsPerOrg(), "Num shards per org must not be null with " + this.getClass().getSimpleName());
		Preconditions.checkNotNull(rotatedIndexMetadata.getNumShards(), "Num shards must not be null with " + this.getClass().getSimpleName());
		this.numShardsPerOrg = rotatedIndexMetadata.getNumShardsPerOrg();
		this.numShards = rotatedIndexMetadata.getNumShards();
		init();
	}

	@Override
	public int getNumShardsPerOrg() {
		return numShardsPerOrg;
	}

	@Override
	public int getNumShards() {
		return numShards;
	}
	

	public Map<Integer, Integer> getShardToRout() {
		return shardToRout;
	}

	/**
	 * For an orgId & convId, get the shard routing for a document.  
	 * 
	 * Note: ES re-hashes routing values so shard 1 wont necessarily mean 
	 * your data ends up in shard 1. However, if you realize that 
	 * then you're in a bad place. 
	 * 
	 * 
	 * @param orgId
	 * @param convId
	 * @param numShards
	 * @param numShardsPerOrg
	 * @return
	 */

	@Override
	public String getRoutingHash(String orgId, String convId) {
		Preconditions.checkArgument(numShards >= numShardsPerOrg, "Misconfigured, numShards must be >= numShardsPerOrg");
		int orgIdHash = getOrgIdHash(orgId, numShards);
		int convIdHash = Math.abs(MurmurHash.getInstance().hash(convId.getBytes(), 0)) % numShardsPerOrg;

		int shard = orgIdHash + convIdHash;
		while(shard >= numShards) {
			shard -= numShards;
		}

		return shardToRout.get(shard).toString();
	}

	/**
	 * When searching data for an Org, you may desire to only search the shards
	 * which hold data for that Org. This gives you a list of possible shard routings.
	 * 
	 * @param orgId
	 * @param numShards
	 * @param numShardsPerOrg
	 * @return
	 */
	@Override
	public String[] getPossibleRoutingHashes(String orgId) {
		int orgIdHash = getOrgIdHash(orgId, numShards);
		String[] possibleShards = new String[numShardsPerOrg];
		for(int x = 0; x < numShardsPerOrg; x ++) {
			int shard = orgIdHash + x;
			while(shard >= numShards) {
				shard -= numShards;
			}
			possibleShards[x] = shardToRout.get(shard).toString();
		}
		return possibleShards;
	}

	private int getOrgIdHash(String orgId, int numShards) {
		return Math.abs(MurmurHash.getInstance().hash(orgId.getBytes(), 0)) % numShards;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + numShards;
		result = prime * result + numShardsPerOrg;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ElasticsearchRoutingStrategyV1 other = (ElasticsearchRoutingStrategyV1) obj;
		if (numShards != other.numShards)
			return false;
		if (numShardsPerOrg != other.numShardsPerOrg)
			return false;
		return true;
	}

    @Override
    public String toString() {
        return "ElasticsearchRoutingStrategyV1 [numShardsPerOrg=" + numShardsPerOrg + ", numShards=" + numShards
                + ", shardToRout=" + shardToRout + "]";
    }

	
}
