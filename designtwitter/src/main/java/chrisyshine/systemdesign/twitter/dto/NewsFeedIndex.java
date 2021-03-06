package chrisyshine.systemdesign.twitter.dto;

/**
 * 
 * @author chrisyshine@gmail.com
 *
 * create table if not exists twitter.news_feed_index (
 * 	user_id text,
 * 	timestamp bigint,
 * 	feed_id text,
 *  primary key ((user_id), timestamp, feed_id)
 * );
 *
 */


@Entity("news_feed_index")
public class NewsFeedIndex {
	@Key(isPartitionKey = true)
	private String userId;
	@Key
	private long timestamp;
	@Key
	private String feedId;
	public NewsFeedIndex() {
	}
	public NewsFeedIndex(String userId, long timestamp, String feedId) {
		this.userId = userId;
		this.timestamp = timestamp;
		this.feedId = feedId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getFeedId() {
		return feedId;
	}
	public void setFeedId(String feedId) {
		this.feedId = feedId;
	}
	
	
	
}
