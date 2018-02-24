package chrisyshine.systemdesign.twitter.dao;

import java.util.List;

import chrisyshine.systemdesign.twitter.dto.User;
import chrisyshine.systemdesign.twitter.kva.KeyValueAccess;

public class UserDao {
	KeyValueAccess kva;
	
	public UserDao(KeyValueAccess kva) {
		this.kva = kva;
	}
	
	public List<User> getAll() {
		return kva.getAll(User.class);
	}
}
