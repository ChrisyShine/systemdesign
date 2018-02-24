package chrisyshine.systemdesign.twitter.dto;

/**
 * 
 * @author chrisyshine@gmail.com
 *
 *
 * create table if not exists twitter.follower_index (
 * 	target_id text,
 * 	follower_id text,
 *  primary key ((target_id), follower_id)
 * );
 *
 */


@Entity("follower_index")
public class FollowerIndex {
	@Key(isPartitionKey = true)
	private String targetId;
	@Key
	private String followerId;
	
	public FollowerIndex() {
	}
	
	public FollowerIndex(String targetId, String followerId) {
		this.targetId = targetId;
		this.followerId = followerId;
	}
	
	public String getTargetId() {
		return targetId;
	}
	
	public void setTargetId(String targetId) {
		this.targetId = targetId;
	}
	
	public String getFollowerId() {
		return followerId;
	}
	
	public void setFollowerId(String followerId) {
		this.followerId = followerId;
	}
	
	
}
