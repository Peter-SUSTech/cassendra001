package com.cassandra.data.CreateData;

/**
 * 数据库静态连接信息
 * @author zhangmalin
 *
 */

public interface CassandraProperties {
	final static String[] CONTACT_POINTS = {"10.20.31.122"};
	final static int PORT = 9042;
	final static String keyspace_name = "cloud_test";
}
