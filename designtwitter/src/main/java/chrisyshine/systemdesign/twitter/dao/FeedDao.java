package chrisyshine.systemdesign.twitter.dao;

import java.util.List;
import java.util.UUID;

import chrisyshine.systemdesign.twitter.dto.Feed;
import chrisyshine.systemdesign.twitter.dto.MyOwnFeedIndex;
import chrisyshine.systemdesign.twitter.dto.NewsFeedIndex;
import chrisyshine.systemdesign.twitter.kva.KeyValueAccess;

public class FeedDao {
	KeyValueAccess kva;
	
	public FeedDao(KeyValueAccess kva) {
		this.kva = kva;
	}
	
	public Feed addFeed(String userId, String content) {
		Feed feed = new Feed(UUID.randomUUID().toString(), userId, System.currentTimeMillis(), content);
		kva.insert(feed);
		return feed;
	}
	
	public List<Feed> getFeedsById(List<String> ids) {
		return kva.getMultiple(ids, Feed.class);
	}
	
	public List<NewsFeedIndex> getNewsFeeds(String userId, int limit) {
		return kva.getByPartitionKey(userId, limit, "timestamp", true, NewsFeedIndex.class);
	}
	
	public List<MyOwnFeedIndex> getMyOwnFeeds(String userId) {
		List<MyOwnFeedIndex> feedIndices = kva.getByPartitionKey(userId, MyOwnFeedIndex.class);
		return feedIndices;
	}
	
	public void insert(List<NewsFeedIndex> indices) {
		kva.insertMultiple(indices);
	}
	
	public void insert(NewsFeedIndex index) {
		kva.insert(index);
	}
	
	public void insert(MyOwnFeedIndex index) {
		kva.insert(index);
	}
	
	public void delete(List<NewsFeedIndex> indices) {
		kva.deleteMultiple(indices);
	}
}
