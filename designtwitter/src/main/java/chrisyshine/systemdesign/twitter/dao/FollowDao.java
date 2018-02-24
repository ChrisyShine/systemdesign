package chrisyshine.systemdesign.twitter.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import chrisyshine.systemdesign.twitter.dto.FollowerIndex;
import chrisyshine.systemdesign.twitter.kva.KeyValueAccess;

public class FollowDao {
	KeyValueAccess kva;
	
	public FollowDao(KeyValueAccess kva) {
		this.kva = kva;
	}
	
	public void insert(String followerId, String targetId) {
		FollowerIndex dto = new FollowerIndex(targetId, followerId);
		kva.insert(dto);
	}
	
	public boolean isFollow(String followerId, String targetId) {
		FollowerIndex dto = kva.get(new FollowerIndex(targetId, followerId), FollowerIndex.class);
		return dto != null;
	}
	
	public void delete(String followerId, String targetId) {
		FollowerIndex dto = new FollowerIndex(targetId, followerId);
		kva.delete(dto);
	}
	
	public Set<String> getFollowerIds(String targetId) {
		List<FollowerIndex> followerIndices = kva.getByPartitionKey(targetId, FollowerIndex.class);
		Set<String> set = new HashSet<String>();
		set.addAll(followerIndices.stream().map(FollowerIndex::getFollowerId).collect(Collectors.toList()));
		return set;
	}
}
