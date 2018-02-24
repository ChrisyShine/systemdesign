package chrisyshine.systemdesign.twitter.dto;

/**
 * 
 * @author chrisyshine@gmail.com
 *
 * create table if not exists twitter.account (
 * 	id text,
 * 	name text,
 *  primary key (id)
 * );
 */

@Entity("account")
public class User {
	@Key(isPartitionKey = true)
	private String id;
	private String name;
	
	public User() {
		
	}
	
	public User(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	
	
}
