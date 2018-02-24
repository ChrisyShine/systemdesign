package chrisyshine.systemdesign.twitter.dto;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 * @author chrisyshine@gmail.com
 *
 *
 * create table if not exists twitter.feed (
 * 	id text,
 * 	timestamp bigint,
 * 	content text,
 *  primary key (id)
 * );
 *
 */


@Entity("feed")
public class Feed {
	@Key(isPartitionKey = true)
	private String id;
	private String userId;
	private long timestamp;
	private String content;
	
	public Feed() {
	}

	public Feed(String id, String userId, long timestamp, String content) {
		this.id = id;
		this.userId = userId;
		this.timestamp = timestamp;
		this.content = content;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	
	public String stringify() {
		StringBuffer sb = new StringBuffer();
		sb.append("userId:");
		sb.append(userId);
		sb.append(", ");
		sb.append("time:");
//		sb.append(timestamp);
//		sb.append(" ");
		Timestamp time = new Timestamp(timestamp);
		sb.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time));
		sb.append(", ");
		sb.append(content);
		return sb.toString();
	}
	
}
