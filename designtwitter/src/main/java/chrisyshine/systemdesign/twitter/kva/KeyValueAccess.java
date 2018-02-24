package chrisyshine.systemdesign.twitter.kva;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import chrisyshine.systemdesign.twitter.dto.Entity;
import chrisyshine.systemdesign.twitter.dto.Key;

public class KeyValueAccess {
	private static final String KEYSPACE = "twitter";
	Cluster cluster = null;
	
	public KeyValueAccess() {
		cluster = Cluster.builder()
	            .addContactPoint("127.0.0.1")
	            .build();
	}
	
	public void close() {
		if (cluster != null) cluster.close();
	}
	
	public <T> List<T> getAll(Class<T> clazz) {
		String columnFamilyName = this.getColumnFamilyName(clazz);
		String keyName = this.getPartitionKeyName(clazz);
		List<T> list = new ArrayList<T>();
		if (columnFamilyName != null && keyName != null) {
			String statement = String.format("select * from %s.%s", KEYSPACE, columnFamilyName, keyName);
			ResultSet rs = execute(statement);
			rs.forEach(row -> {
				list.add(matchRow(row, clazz));
			});
		}
		return list;
	}
	
	public <T> List<T> getByPartitionKey(String key, Class<T> clazz) {
		String columnFamilyName = this.getColumnFamilyName(clazz);
		String keyName = this.getPartitionKeyName(clazz);
		List<T> list = new ArrayList<T>();
		if (columnFamilyName != null && keyName != null) {
			String statement = String.format("select * from %s.%s where %s = '%s'", KEYSPACE, columnFamilyName, keyName, key);
			ResultSet rs = execute(statement);
			rs.forEach(row -> {
				list.add(matchRow(row, clazz));
			});
		}
		return list;
	}
	
	public <T> List<T> getByPartitionKey(String key, int limit, String orderBy, boolean desc, Class<T> clazz) {
		String columnFamilyName = this.getColumnFamilyName(clazz);
		String keyName = this.getPartitionKeyName(clazz);
		List<T> list = new ArrayList<T>();
		if (columnFamilyName != null && keyName != null) {
			String statement = String.format(
					"select * from %s.%s where %s = '%s' order by %s %s limit %d", 
					KEYSPACE, columnFamilyName, keyName, key, orderBy, desc?"DESC":"ASC", limit);
			ResultSet rs = execute(statement);
			rs.forEach(row -> {
				list.add(matchRow(row, clazz));
			});
		}
		return list;
	}
	
	private <T> String getCondition(T dto) {
		Class<?> clazz = dto.getClass();
		Map<String, Object> columnValueMap = this.getColumnValueMap(dto);
		StringBuffer condition = new StringBuffer();
		Field[] fields = clazz.getDeclaredFields();
		for(Field field:fields) {
			if (field.isAnnotationPresent(Key.class)) {
				String key = field.getName();
				if (condition.length() > 0) {
					condition.append(" and ");
				}
				condition.append(key.replaceAll("([^_A-Z])([A-Z])", "$1_$2").toLowerCase());
				condition.append("=");
				Object value = columnValueMap.get(key);
				if (value instanceof String) {
					condition.append("'");
					condition.append(value);
					condition.append("'");
				} else {
					condition.append(value);
				}
			}
		}
		return condition.toString();
	}
	
	public <T> T get(T dto, Class<T> clazz) {
		String columnFamilyName = this.getColumnFamilyName(clazz);
		String keyName = this.getPartitionKeyName(clazz);
		if (columnFamilyName != null && keyName != null) {
			
			StringBuffer sb = new StringBuffer();
			sb.append(String.format("select * from %s.%s where ", KEYSPACE, columnFamilyName, keyName));
			sb.append(getCondition(dto));
			
			String statement = sb.toString();
			ResultSet rs = execute(statement);
			Row row = rs.one();
			return matchRow(row, clazz);
		}
		return null;
	}
	
	
	public <T> void delete(T dto) {
		Class<?> clazz = dto.getClass();
		String columnFamilyName = this.getColumnFamilyName(clazz);
		if (columnFamilyName != null) {
			StringBuffer sb = new StringBuffer();
			sb.append(String.format("delete from %s.%s where ", KEYSPACE, columnFamilyName));
			sb.append(getCondition(dto));
			
			String statement = sb.toString();
			execute(statement);
		}
	}
	
	public <T> void deleteMultiple(List<T> dtos) {
		if (dtos == null || dtos.size() == 0) {
			return;
		}
		T dto = dtos.get(0);
		Class<?> clazz = dto.getClass();
		String columnFamilyName = this.getColumnFamilyName(clazz);
		List<String> statements = dtos.stream()
				.map(one -> {
					StringBuffer sb = new StringBuffer();
					sb.append(String.format("delete from %s.%s where ", KEYSPACE, columnFamilyName));
					sb.append(getCondition(one));
					return sb.toString();
				}).collect(Collectors.toList());
		execute(statements);
	}

	private <T> String getColumnFamilyName(Class<T> clazz) {
		String columnFamilyName = null;
		if (clazz.isAnnotationPresent(Entity.class)) {
			Entity entity = (Entity)clazz.getAnnotation(Entity.class);
			columnFamilyName = entity.value();
		}
		return columnFamilyName;
	}
	
	private <T> String getPartitionKeyName(Class<T> clazz) {
		String keyName = null;
		Field[] fields = clazz.getDeclaredFields();
		for(Field field:fields) {
			if (field.isAnnotationPresent(Key.class)) {
				Key key = field.getAnnotation(Key.class);
				if (key.isPartitionKey()) {
					keyName = field.getName().replaceAll("([^_A-Z])([A-Z])", "$1_$2").toLowerCase();
					break;
				}
			}
		}
		return keyName;
	}
	
	public <T> List<T> getMultiple(List<String> keys, Class<T> clazz) {
		List<T> list = new ArrayList<T>();
		String columnFamilyName = this.getColumnFamilyName(clazz);
		String keyName = this.getPartitionKeyName(clazz);
		if (columnFamilyName != null && keyName != null) {
			List<String> statements = new ArrayList<String>();
			for(String key:keys) {
				statements.add(String.format("select * from %s.%s where %s = '%s'", KEYSPACE, columnFamilyName, keyName, key));
			}
			List<ResultSetFuture> rs = execute(statements);
			for (ResultSetFuture future : rs){
				 ResultSet rows = future.getUninterruptibly();
				 Row row = rows.one();
				 list.add(matchRow(row, clazz));
			}
		}
		return list;
	}
	
	private <T> T matchRow(Row row, Class<T> clazz) {
		if (row == null) {
			return null;
		}
		Method[] methods = clazz.getMethods();
		Map<String, Method> map = new HashMap<String, Method>();
		for(Method method:methods) {
			if (method.getName().startsWith("set")) {
				String methodName = method.getName();
				methodName = StringUtils.uncapitalize(methodName.substring(3));
				map.put(methodName, method);
			}
		}
		try {
			final T dto = clazz.newInstance();
			ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
			columnDefinitions.forEach(definition -> {
				String name = definition.getName();
				DataType dataType = definition.getType();
				Object value = null;
				String camelName = StringUtils.uncapitalize(StringUtils.remove(WordUtils.capitalizeFully(name, '_'), "_"));
				
				if (dataType.getName().name().equals("VARCHAR")) {
					value = row.getString(name);
				} else if (dataType.getName().name().equals("BIGINT")) {
					value = row.getLong(name);
				}
				
				if (value != null) {
					Method method = map.get(camelName);
					if (method != null) {
						try {
							method.invoke(dto, value);
						} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							e.printStackTrace();
						}
					}
				}
			});
			return dto;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private <T> Map<String, Object> getColumnValueMap(T dto) {
		Class<?> clazz = dto.getClass();
		Method[] methods = clazz.getMethods();
		Map<String, Method> methodMap = new HashMap<String, Method>();
		for(Method method:methods) {
			if (method.getName().startsWith("get")) {
				String methodName = method.getName();
				methodName = StringUtils.uncapitalize(methodName.substring(3));
				methodMap.put(methodName, method);
			}
		}
		
		Map<String, Object> columnValueMap = new HashMap<String, Object>();
		
		for(Field field:clazz.getDeclaredFields()) {
			String fieldName = field.getName();
			Method method = methodMap.get(fieldName);
			if (method != null) {
				try {
					Object value = method.invoke(dto);
					columnValueMap.put(fieldName, value);
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					e.printStackTrace();
				}
			}
			
		}
		return columnValueMap;
	}
	
	private <T> String getInsertStatement(String columnFamilyName, T dto) {
		Map<String, Object> columnValueMap = this.getColumnValueMap(dto);
		StringBuffer columnNameSb = new StringBuffer();
		StringBuffer valueSb = new StringBuffer();
		for(String columnName:columnValueMap.keySet()) {
			if (columnNameSb.length() > 0) {
				columnNameSb.append(",");
				valueSb.append(",");
			}
			columnNameSb.append(columnName.replaceAll("([^_A-Z])([A-Z])", "$1_$2").toLowerCase());
			Object value = columnValueMap.get(columnName);
			if (value instanceof String) {
				valueSb.append("'");
				valueSb.append(value);
				valueSb.append("'");
			} else {
				valueSb.append(value);
			}
		}
		
		
		StringBuffer sb = new StringBuffer();
		sb.append("insert into ");
		sb.append(KEYSPACE);
		sb.append(".");
		sb.append(columnFamilyName);
		sb.append(" (");
		sb.append(columnNameSb.toString());
		sb.append(") values (");
		sb.append(valueSb.toString());
		sb.append(");");
		return sb.toString();
	}
	
	public <T> void insert(T dto) {
		Class<?> clazz = dto.getClass();
		String columnFamilyName = this.getColumnFamilyName(clazz);
		String statement = getInsertStatement(columnFamilyName, dto);
		execute(statement);
	}
	
	
	public <T> void insertMultiple(List<T> dtos) {
		if (dtos == null || dtos.size() == 0) {
			return;
		}
		T dto = dtos.get(0);
		Class<?> clazz = dto.getClass();
		String columnFamilyName = this.getColumnFamilyName(clazz);
		List<String> statements = dtos.stream().map(one -> getInsertStatement(columnFamilyName, one)).collect(Collectors.toList());
		execute(statements);
	}
	
	private ResultSet execute(String statement) {
	    Session session = cluster.connect();
	    System.out.println(statement);
	    ResultSet rs = session.execute(statement);
	    return rs;
	}
	
	private List<ResultSetFuture> execute(List<String> statements) {
	    Session session = cluster.connect();
	    
	    List<ResultSetFuture> rs = new ArrayList<ResultSetFuture>();
	    for(String statement:statements) {
	    	System.out.println(statement);
	    	rs.add(session.executeAsync(statement));
	    }
	    return rs;
	}
	
}
