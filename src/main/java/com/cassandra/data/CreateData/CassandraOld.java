package com.cassandra.data.CreateData;

/**
 * 试验于老的服务器上的数据库
 * @author zhangmalin
 */

import java.util.Random;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraOld {

    private Cluster cluster;
    private Session session;
	private Random rand;
	
	public CassandraOld() {
		rand = new Random();	
	}
    
    public void connect(String[] contactPoints, int port) {

        cluster = Cluster.builder()
                .addContactPoints(contactPoints).withPort(port)
                .build();

        System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());
        session = cluster.connect("new_test");	// 要求先创建好键空间
    }
    
    public void CreateUserTable() {
    	UserTables();
    }
    
    public void DeleteTable() {
    	DeleteUserTables();
    }
    
    public void close() {
        session.close();
        cluster.close();
    }
    
    private void DeleteUserTables() {
    	session.execute("DROP TABLE IF EXISTS \"UserProjects\";");
    	session.execute("DROP TABLE IF EXISTS \"Roles\";");
    	session.execute("DROP TABLE IF EXISTS \"Team\";");
    	session.execute("DROP TABLE IF EXISTS \"UserTeamRole\";");
    	session.execute("DROP TABLE IF EXISTS \"TeamProjects\";");
    	session.execute("DROP TABLE IF EXISTS user;");
    	session.execute("DROP TABLE IF EXISTS \"Role\";");
    	session.execute("DROP TABLE IF EXISTS \"RoleMapping\";");
    	session.execute("DROP TABLE IF EXISTS \"AccessToken\";");
    	session.execute("DROP TABLE IF EXISTS \"ACL\";");
    	session.execute("DROP TABLE IF EXISTS \"User\";");
    }
    
    private void UserTables() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS userproject (" +
                        "user_project_id uuid," +
                        "\"userId\" uuid," +
                        "project_id uuid," +
                        "PRIMARY KEY (\"userId\", project_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS \"Roles\" (" +
                		"role_id uuid," +
                        "name text," +
                        "description text," +
                        "created timestamp," +
                        "modified timestamp," +
                        "PRIMARY KEY (role_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS team (" +
                		"team_id uuid," +
                        "team_name text," +
                        "PRIMARY KEY (team_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS userteamrole (" +
                		"user_team_role_id uuid," +
                        "\"userId\" uuid," +
                        "team_id uuid," +
                        "role_id uuid," +
                        "\"principalType\" text," +
                        "\"principalId\" text," +
                        "PRIMARY KEY (\"userId\", team_id, role_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS teamproject (" +
                		"team_project_id uuid," +
                        "team_id uuid," +
                        "project_id uuid," +
                        "PRIMARY KEY (team_id, project_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS user (" +
                		"\"userId\" uuid," +
                        "realm text," +
                        "username text," +
                        "password text," +
                        "email text," +
                        "\"emailVerified\" boolean," +
                        "\"verificationToken\" text," +
                        "PRIMARY KEY (\"userId\")" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS \"Role\" (" +
                		"id text," +
                        "name text," +
                        "description text," +
                        "created timestamp," +
                        "modified timestamp," +
                        "PRIMARY KEY (id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS \"RoleMapping\" (" +
                		"id text," +
                        "\"principalType\" text," +
                        "\"principalId\" text," +
                        "\"roleId\" text," +
                        "PRIMARY KEY (id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS \"AccessToken\" (" +
                		"id text," +
                        "ttl double," +
                        "scopes set<text>," +
                        "created timestamp," +
                        "\"userId\" uuid," +
                        "PRIMARY KEY (id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS \"ACL\" (" +
                		"id text," +
                		"model text," +
                        "property text," +
                        "\"accessType\" text," +
                        "permission text," +
                        "\"principalType\" text," +
                        "\"principalId\" text," +
                        "PRIMARY KEY (property, \"accessType\")" +
                        ");");
    }
}
