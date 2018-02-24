package chrisyshine.systemdesign.twitter.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import chrisyshine.systemdesign.twitter.dao.FeedDao;
import chrisyshine.systemdesign.twitter.dao.FollowDao;
import chrisyshine.systemdesign.twitter.dto.Feed;
import chrisyshine.systemdesign.twitter.dto.MyOwnFeedIndex;
import chrisyshine.systemdesign.twitter.dto.NewsFeedIndex;
import chrisyshine.systemdesign.twitter.kva.KeyValueAccess;

public class Service {
	private static final int LIMIT = 10;
	
	private KeyValueAccess kva;
	private FeedDao feedDao;
	private FollowDao followDao;
	public Service() {
		kva = new KeyValueAccess();
		feedDao = new FeedDao(kva);
		followDao = new FollowDao(kva);
	}
	
	public void close() {
		kva.close();
	}
	
	public void postFeed(String userId, String content) {
		// add feed
		Feed feed = feedDao.addFeed(userId, content);
		
		// add MyOwnFeedIndex
		MyOwnFeedIndex myOwnFeedIndex = new MyOwnFeedIndex(userId, feed.getTimestamp(), feed.getId());
		feedDao.insert(myOwnFeedIndex);
		
		// add NewsFeedIndex for myself and all the followers
		Set<String> userIds = followDao.getFollowerIds(userId);
		userIds.add(userId);
		List<NewsFeedIndex> newsFeedIndices = userIds.stream()
				.map(user -> new NewsFeedIndex(user, feed.getTimestamp(), feed.getId()))
				.collect(Collectors.toList());
		feedDao.insert(newsFeedIndices);
	}
	
	public boolean follow(String followerId, String targetId) {
		if (followerId == null || targetId == null || followerId.equals(targetId)) {
			return false;
		}
		if (followDao.isFollow(followerId, targetId)) {
			return false;
		}
		// add followerIndex
		followDao.insert(followerId, targetId);
		// add news feed
		List<MyOwnFeedIndex> feedIndices = feedDao.getMyOwnFeeds(targetId);
		List<NewsFeedIndex> indices = feedIndices.stream().map(index -> {
			return new NewsFeedIndex(followerId, index.getTimestamp(), index.getFeedId());
		}).collect(Collectors.toList());
		feedDao.insert(indices);
		return true;
	}
	
	public boolean unFollow(String followerId, String targetId) {
		if (followerId == null || targetId == null || followerId.equals(targetId)) {
			return false;
		}
		if (!followDao.isFollow(followerId, targetId)) {
			return false;
		}
		
		// remove followerIndex
		followDao.delete(followerId, targetId);
		
		// remove news feed
		List<MyOwnFeedIndex> feedIndices = feedDao.getMyOwnFeeds(targetId);
		List<NewsFeedIndex> indices = feedIndices.stream().map(index -> {
			return new NewsFeedIndex(followerId, index.getTimestamp(), index.getFeedId());
		}).collect(Collectors.toList());
		feedDao.delete(indices);
		
		return true;
	}
	
	public List<Feed> getNewsFeed(String userId) {
		List<NewsFeedIndex> indices = feedDao.getNewsFeeds(userId, LIMIT);
		List<String> feedIds = indices.stream().map(NewsFeedIndex::getFeedId).collect(Collectors.toList());
		List<Feed> feeds = feedDao.getFeedsById(feedIds);
		Map<String, Feed> map = new HashMap<String, Feed>();
		for(Feed feed:feeds) {
			map.put(feed.getId(), feed);
		}
		
		// rearrange feeds according to index order
		List<Feed> result = feedIds.stream().map(id -> map.get(id)).collect(Collectors.toList());
		
		return result;
	}
	
	
	
}
