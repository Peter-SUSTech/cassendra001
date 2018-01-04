package com.cassandra.data.CreateData;

/**
 * 生成模拟数据的类
 * @author zhangmalin
 */

import java.util.Random;
import java.util.UUID;
import java.util.Arrays;
import java.util.HashMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;


public final class CassandraDemo {		// 如果将CassandraDemo继承TestData接口，则不需要写上TestData作用域来引用数据
    private Cluster cluster;
    private Session session;
	private Random rand;
	
	// 默认构造函数
	public CassandraDemo() {
		rand = new Random();	
	}
    
    public void connect(String[] contactPoints, int port) {

        cluster = Cluster.builder()
                .addContactPoints(contactPoints).withPort(port)
                .build();

        System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());
        
//      session.execute("CREATE KEYSPACE IF NOT EXISTS fire_cloud WITH replication " +
//      "= {'class':'SimpleStrategy', 'replication_factor':3};");
        session = cluster.connect(CassandraProperties.keyspace_name);	// 要求先创建好键空间
    }
    
    // 创建基础信息数据数据表
    public void BuildData() {
    	CreateStaticTables();
    	CreateTables();
    	InsertData();
    }
    
    // 创建消防设备的统计信息表
    public void BuildStat() {
    	StateStatTable();
    	createEventStat();
    }
    
    // 创建用户管理权限的数据表
    public void UserManage() {
    	CreateUserTable();
    }
    
    public void DeleteTables() {
    	DeleteStatData();
    }
    
    public void ChangeTables() {
    	AlterTables();
    }
    
    // 添加维保人员信息及相关数据表
    public void AddMaintainers() {
    	BuildMaintainerTables();
    }
    
    // 构建设备号与通信协议中的对应关系，用于确立所接收到的消息属于哪个设备
    public void BuildDeviceMap() {
    	createDeviceMap();
    }
    
    public void createIndex() {
        session.execute(
        		"CREATE INDEX ON devicemaintainer(device_id);");
//        session.execute(
//        		"CREATE INDEX ON maintainer(maintainer_phone);");
    }
    
    private void BuildMaintainerTables() {
    	// create maintainer
        session.execute(
                "CREATE TABLE IF NOT EXISTS maintainer (" +
                        "maintainer_id uuid," +
                        "maintainer_name text," +
                        "maintainer_phone text," +
                        "maintainer_info text," +
                        "PRIMARY KEY (maintainer_id)" +
                        ");");
        // add datas
        String[] maintainerUUID = getUUID(3);
        String[] phone_number = {"18018763747", "13008864027"};
        String[] name = {"zhangmalin", "wujing"};
        // 如果是字符串变量，需要在格式化字符中添加单引号
        session.execute(String.format("INSERT INTO maintainer (maintainer_id, maintainer_name, maintainer_phone, maintainer_info) " +
				"VALUES (" + "%s," +"'%s'," +"'%s'," +"'%s'" +");", maintainerUUID[0], name[0], phone_number[0], "maintainerinfo1"));
        session.execute(String.format("INSERT INTO maintainer (maintainer_id, maintainer_name, maintainer_phone, maintainer_info) " +
				"VALUES (" + "%s," +"'%s'," +"'%s'," +"'%s'" +");", maintainerUUID[1], name[1], phone_number[1], "maintainerinfo2"));
        
        // 创建设备和维保人员的多对多的关系数据表
        session.execute(
                "CREATE TABLE IF NOT EXISTS devicemaintainer (" +
                        "id uuid," +
                        "device_id uuid," +
                        "maintainer_id uuid," +
                        "PRIMARY KEY (id)" +
                        ");");
        ResultSet deviceIds = session.execute(String.format("SELECT device_id FROM devices;"));
        deviceIds.one().getUUID("device_id").toString();
        String[] UUID = getUUID(10010);
        int i = 0;
        for (Row devicerow : deviceIds)
        {
        	String deviceUUID = devicerow.getUUID("device_id").toString();
        	session.execute(String.format("INSERT INTO devicemaintainer (id, device_id, maintainer_id) " +
    				"VALUES (" + "%s," +"%s," +"%s" +");", UUID[i++], deviceUUID, maintainerUUID[rand.nextInt(2)]));
        }
        
    }
    
    private void AlterTables() {
    	session.execute("ALTER TABLE userproject ADD team_id uuid;");
    }
    
    private void CreateUserTable() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS userproject (" +
                        "user_project_id uuid," +
                        "\"userId\" uuid," +
                        "project_id uuid," +
                        "PRIMARY KEY (user_project_id)" +
                        ");");
        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS \"Roles\" (" +
//                		"role_id uuid," +
//                        "name text," +
//                        "description text," +
//                        "created timestamp," +
//                        "modified timestamp," +
//                        "PRIMARY KEY (role_id)" +
//                        ");");
//        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS team (" +
//                		"team_id uuid," +
//                        "team_name text," +
//                        "PRIMARY KEY (team_id)" +
//                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS userteamrole (" +
                		"user_team_role_id uuid," +
                        "\"userId\" uuid," +
                        "team_id uuid," +
                        "role_id uuid," +
                        "\"principalType\" text," +
                        "\"principalId\" text," +
                        "PRIMARY KEY (user_team_role_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS teamproject (" +
                		"team_project_id uuid," +
                        "team_id uuid," +
                        "project_id uuid," +
                        "PRIMARY KEY (team_project_id)" +
                        ");");
        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS user (" +
//                		"\"userId\" uuid," +
//                        "realm text," +
//                        "username text," +
//                        "password text," +
//                        "email text," +
//                        "\"emailVerified\" boolean," +
//                        "\"verificationToken\" text," +
//                        "PRIMARY KEY (\"userId\")" +
//                        ");");
//        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS \"Role\" (" +
//                		"id text," +
//                        "name text," +
//                        "description text," +
//                        "created timestamp," +
//                        "modified timestamp," +
//                        "PRIMARY KEY (id)" +
//                        ");");
//        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS \"RoleMapping\" (" +
//                		"id text," +
//                        "\"principalType\" text," +
//                        "\"principalId\" text," +
//                        "\"roleId\" text," +
//                        "PRIMARY KEY (id)" +
//                        ");");
//        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS \"AccessToken\" (" +
//                		"id text," +
//                        "ttl double," +
//                        "scopes set<text>," +
//                        "created timestamp," +
//                        "\"userId\" uuid," +
//                        "PRIMARY KEY (id)" +
//                        ");");
//        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS \"ACL\" (" +
//                		"id text," +
//                		"model text," +
//                        "property text," +
//                        "\"accessType\" text," +
//                        "permission text," +
//                        "\"principalType\" text," +
//                        "\"principalId\" text," +
//                        "PRIMARY KEY (property, \"accessType\")" +
//                        ");");
        
    }
    
    private void DeleteStatData() {
//    	session.execute("DROP TABLE IF EXISTS userprojects;");
//    	session.execute("DROP TABLE IF EXISTS role;");
//    	session.execute("DROP TABLE IF EXISTS team;");
//    	session.execute("DROP TABLE IF EXISTS userteamrole;");
//    	session.execute("DROP TABLE IF EXISTS teamprojects;");
//    	session.execute("DROP TABLE IF EXISTS user;");
//    	session.execute("DROP KEYSPACE cloud_demo;");
    	session.execute("DROP TABLE IF EXISTS userproject;");
//    	session.execute("DROP TABLE IF EXISTS \"Roles\";");
//    	session.execute("DROP TABLE IF EXISTS \"Team\";");
    	session.execute("DROP TABLE IF EXISTS userteamrole;");
    	session.execute("DROP TABLE IF EXISTS teamproject;");
//    	session.execute("DROP TABLE IF EXISTS user;");
//    	session.execute("DROP TABLE IF EXISTS \"Role\";");
//    	session.execute("DROP TABLE IF EXISTS \"RoleMapping\";");
//    	session.execute("DROP TABLE IF EXISTS \"AccessToken\";");
//    	session.execute("DROP TABLE IF EXISTS \"ACL\";");
//    	session.execute("DROP TABLE IF EXISTS \"User\";");
    }
    
    
    private void createDeviceMap()
    {
        session.execute(
                "CREATE TABLE IF NOT EXISTS devicemap (" +
                        "display_id uuid," +
                        "controller smallint," +
                        "unit smallint," +
                        "device_code smallint," +
                        "road smallint," +
                        "device_id uuid," +
                        "PRIMARY KEY (display_id, controller, unit, device_code, road)" +
                        ");");
    	
    	ResultSet projectIds = session.execute("SELECT project_id FROM projects;");
    	for (Row project_row : projectIds)		// 以project_id作为display_id,即图显个数为10
    	{
    		String projectUUID = project_row.getUUID("project_id").toString();
    		ResultSet deviceIds = session.execute(String.format("SELECT device_id FROM devices WHERE project_id=%s ALLOW FILTERING;", projectUUID));
    		
    		for (int controller = 0; controller < 10; controller++) // 控制器个数为10
    		{
    			for (int unit = 0; unit < 10; unit++) //单元个数为10
    			{
    				for (int device_code = 0; device_code < 5; device_code++) //设备编号个数为5
    				{
    					for (int road = 0; road < 2; road++) // 通道个数为2
    					{
							session.execute(String.format("INSERT INTO devicemap (display_id, controller, unit, device_code, road, device_id) " +
	    							"VALUES (" + "%s," +"%d," +"%d," +"%d," +"%d," +"%s" +");", projectUUID, controller, unit, device_code, road, deviceIds.one().getUUID("device_id").toString()));
    					}
    				}
    			}
    		}
    	} 	    	
    }
    
    private void CreateTables() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS projects (" +
                        "project_id uuid PRIMARY KEY," +
                        "project_name text," +
                        "longitude float," +
                        "latitude float," +
                        "province varchar," +
                        "city varchar," +
                        "district varchar," +
                        "project_address text," +
                        "project_info text" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS devices (" +
                        "project_id uuid," +
                        "building_id uuid," +
                        "floor_id uuid," +
                        "system_id uuid," +
                        "device_id uuid," +
                        "device_name text," +
                        "device_type_id smallint," +
                        "device_info text," +
                        "latest_state text," +
                        "state_time timestamp," +
                        "system_type_id smallint," +
                        "PRIMARY KEY (project_id, building_id, floor_id, device_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS buildings (" +
                        "project_id uuid," +
                        "building_id uuid," +
                        "building_name text," +
                        "building_info text," +
                        "PRIMARY KEY (project_id, building_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS floors (" +
                        "project_id uuid," +
                        "building_id uuid," +
                        "floor_id uuid," +
                        "floor_name text," +
                        "floor_info text," +
                        "PRIMARY KEY (project_id, building_id, floor_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS systems (" +
                        "project_id uuid," +
                        "system_id uuid," +
                        "system_type_id smallint," +
                        "system_info text," +
                        "PRIMARY KEY (project_id, system_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS events (" +
                        "id uuid," +
                        "device_id uuid," +
                        "time timestamp," +
                        "event_level text," +
                        "event_type_id smallint," +
                        "PRIMARY KEY (device_id, time)" +
                        ") WITH CLUSTERING ORDER BY (time DESC);");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS devicestate (" +
                        "id uuid," +
                        "device_id uuid," +
                        "time timestamp," +
                        "state text," +
                        "detail_state text," +
                        "PRIMARY KEY (device_id, time)" +
                        ") WITH CLUSTERING ORDER BY (time DESC);");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS deviceparamdata (" +
                        "id uuid," +
                        "device_id uuid," +
                        "time timestamp," +
                        "param_type_id smallint," +
                        "param_value int," +
                        "PRIMARY KEY (device_id, param_type_id, time)" +
                        ") WITH CLUSTERING ORDER BY (param_type_id ASC, time DESC);");
        
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS deviceposition (" +
                		"id uuid," +
                        "device_id uuid," +
                        "image_id uuid," +
                        "pos_x float," +
                        "pos_y float," +
                        "pos_z float," +
                        "PRIMARY KEY (device_id, image_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS images (" +
                		"id uuid," +
                        "item_id uuid," +
                        "image_id uuid," +
                        "dynamic tinyint," +
                        "extension text," +
                        "image_info text," +
                        "PRIMARY KEY (item_id)" +
                        ");");
        
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS devicemap (" +
//                        "display_id uuid," +
//                        "port smallint," +
//                        "address smallint," +
//                        "controller smallint," +
//                        "unit smallint," +
//                        "device_type_id smallint," +
//                        "device_id uuid," +
//                        "PRIMARY KEY (display_id, port, address, controller, unit, device_type_id)" +
//                        ");");

//        // 暂时取出maker_id，因添加上maker_id将导致无法创建表
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS eventstat (" +
//                        "item_id uuid," +
//                        "date timestamp," +
//                        "system_type_id smallint," +
//                        "device_type_id smallint," +
//                        "event_type_id smallint," +
//                        "urgent_num counter," +
//                        "general_num counter," +
//                        "PRIMARY KEY (item_id, date, system_type_id, device_type_id, event_type_id)" +
//                        ") WITH CLUSTERING ORDER BY (date DESC);");
//
//        // 非主键的项应该都必须是counter,否则无法创建表,先去除update_time项
//        session.execute(
//                "CREATE TABLE IF NOT EXISTS lateststatestat (" +
//                        "item_id uuid," +
//                        "system_type_id smallint," +
//                        "device_type_id smallint," +
//                        "normal_num counter," +
//                        "abnormal_num counter," +
//                        "offline_num counter," +
//                        "PRIMARY KEY (item_id, system_type_id, device_type_id)" +
//                        ");");
        
        //create index project_id for devices table;
        session.execute(
        		"CREATE INDEX ON devices(system_id);");
    }
    
    private void InsertData() {
    	
    	float[] longitude = getrandom(10,100, 118);
    	float[] latitude = getrandom(10,25, 40);

		String[] project_id = getUUID(10);
//		int[][][] project_date = new int[10][2][30];
		for (int i = 0; i < 10; i++)
		{
			String project_cql = null;
			project_cql = String.format("INSERT INTO projects (project_id, project_name, longitude, latitude, province, city, district, project_address, project_info) " +
			"VALUES (" + "%s," +"'%s'," +"%10.6f," +"%10.6f," +"'%s'," +"'%s'," +"'%s'," +"'%s'," +"'%s'" +");", project_id[i], TestData.project_name[i], longitude[i], latitude[i], TestData.province[i],
			TestData.city[i],TestData.district[i],TestData.project_address[i], "project_info_" + i);
			session.execute(project_cql);
			
			//每个项目中包含所有不同类型的系统，且只有一个。
			String[] system_id = getUUID(TestData.system_type_code.length);
			for (int n = 0; n<TestData.system_type_code.length; n++)
			{
				String system_cql = null;
				system_cql = String.format("INSERT INTO systems (project_id, system_id, system_type_id, system_info) " +
						"VALUES (" + "%s," +"%s," +"%d," +"'%s'" +");", project_id[i], system_id[n], TestData.system_type_code[n], "system_info_" + i + n);
				session.execute(system_cql);
			}
			
			String[] building_id = getUUID(5);
			for (int j = 0; j < 5; j++)
			{
				String building_cql = null;
				building_cql = String.format("INSERT INTO buildings (project_id, building_id, building_name, building_info) " +
						"VALUES (" + "%s," +"%s," +"'%s'," +"'%s'" +");", project_id[i], building_id[j], TestData.building_name[j], "building_info_" + i +j);
				session.execute(building_cql);
				
				String[] floor_id = getUUID(10);
				for (int k = 0; k < 10; k++)
				{
					String floor_cql = null;
					floor_cql = String.format("INSERT INTO floors (project_id, building_id, floor_id, floor_name, floor_info) " +
							"VALUES (" + "%s," +"%s," +"%s," +"'%s'," +"'%s'" +");", project_id[i], building_id[j], floor_id[k], TestData.floor_name[k], "floor_info_" + i + j + k);
					session.execute(floor_cql);
					
					String[] device_id = getUUID(20);
					for (int m = 0; m < 20; m++)
					{

						String[] state_id = getUUID(50);
						long[] state_time = getTimestamps(30, 50);
						Arrays.sort(state_time);
						int[] state = SetRandomRate(50);	// 设置正常、异常及离线
						for (int q = 0; q < 50; q++)	// 每个设备生成50个状态值
						{
							String state_cql = String.format("INSERT INTO devicestate (id, device_id, time, state, detail_state) " +
									"VALUES (" + "%s," +"%s," +"%d," +"'%s'," +"'%s'" +");", state_id[q], device_id[m], state_time[q], TestData.device_state[state[q]], TestData.state_names[1][state[q]]);
							session.execute(state_cql);
						}
						
						String device_cql = null;
						int system_type_id = rand.nextInt(TestData.system_type_code.length);	// 为每一个设备分配一个系统类型，但当前并未划分出哪种设备属于哪个类型的系统
						device_cql = String.format("INSERT INTO devices (project_id, building_id, floor_id, system_id, device_id, device_name, device_type_id, device_info, latest_state, state_time, system_type_id) " +
								"VALUES (" + "%s," +"%s," +"%s," +"%s," +"%s," +"'%s'," + "%d," + "'%s'," + "'%s'," + "%d," + "%d" +");", project_id[i], building_id[j], floor_id[k], system_id[system_type_id], device_id[m], 
								"device_name_" + i + j + k + m, TestData.device_code[rand.nextInt(TestData.device_code.length)], "device_info_" + i + j + k + m, TestData.device_state[state[state.length - 1]],
								state_time[state_time.length -1], TestData.system_type_code[system_type_id]);
						session.execute(device_cql);
											
						// 每个设备生成过去30天内的50个事件
						String[] event_id = getUUID(50);
						long[] event_time = getTimestamps(30, 50);
//						Arrays.sort(event_time);
						for (int p = 0; p < 50; p++)
						{
							int event_code_id = rand.nextInt(TestData.event_code.length);
							String event_cql = String.format("INSERT INTO events (id, device_id, time, event_level, event_type_id) " +
									"VALUES (" + "%s," +"%s," +"%d," +"'%s'," +"%d" +");", event_id[p], device_id[m], event_time[p], EventLevel(event_code_id), TestData.event_code[event_code_id]);
							session.execute(event_cql);
						}
						
					}
				}
			}
		}
    }
    
    private long[] getTimestamps(int dates, int num)
    {
    	if (num <1){return null;}
    	long now_time = System.currentTimeMillis();
    	long[] time_stamps = new long[num];
    	for (int i = 0; i < num; i++)
    	{
    		time_stamps[i] = now_time - (long)(rand.nextDouble() * dates * 86400000L);
    	}
    	return time_stamps;
    }

    private static String[] getUUID(int number)
    {
    	if (number < 1) {
    		return null;
    		}
    	String[] str = new String[number];
    	for (int i = 0; i < number; i++)
    	{
    		str[i] = UUID.randomUUID().toString();
    	}
    	return str;
    }
    
    private static float[] getrandom(int num, int min, int max)
    {
    	float[] position = new float[num];
    	for (int i= 0; i < num; i++)
    	{
    		position[i] = min + ((max - min) * new Random().nextFloat());
    	}
    	return position;
    }
    
    
    private int[] SetRandomRate(int num) // 设置设备的num个状态
    {
    	// 6。3.1的比例来生成正常、异常和离线的设备
    	int[] value = new int[num];
    	for (int i = 0; i < num; i++)
    	{
        	switch (rand.nextInt(10))
        	{
        	case 0:
        	case 1:
        	case 2:
        	case 3:
        	case 4:
        	case 5:
        		value[i] = 0;
        		break;
        	case 6:
        	case 7:
        	case 8:
        		value[i] = 1;
        		break;
        	case 9:
        		value[i] = 2;
        		break;
    		default:
    			break;
        	}
    	}
    	return value;
    }
    
    private String EventLevel(int event_index)
    {
    	if (event_index == 0)
    	{
    		return "正常";
    	}
    	else if (event_index <= TestData.urgent_code.length)
    	{
    		return "紧急";
    	}
    	else
    	{
    		return "一般";
    	}
    }
    
    //创建状态统计表
    private void StateStatTable()
    {
        session.execute(
                "CREATE TABLE IF NOT EXISTS lateststatestat (" +
                        "item_id uuid," +
                        "system_type_id smallint," +
                        "device_type_id smallint," +
                        "update_time timestamp," +
                        "normal_num int," +
                        "abnormal_num int," +
                        "offline_num int," +
                        "PRIMARY KEY (item_id, system_type_id, device_type_id)" +
                        ");");

    	int[] devicestate_flag = new int[3];
    	ResultSet devicenames = session.execute("SELECT * FROM devices;");//此处可能无法查询到全部，只显示5000个。
    	
//        	long[] latest_time = new long[5];//用于保存
    	for (Row devicerow : devicenames)
    	{
    		String projectUUID = devicerow.getUUID("project_id").toString();
//    		String deviceUUID = devicerow.getUUID("device_id").toString();
    		String buildingUUID = devicerow.getUUID("building_id").toString();
    		String floorUUID = devicerow.getUUID("floor_id").toString();
    		String systemUUID = devicerow.getUUID("system_id").toString();
    		short system_type = devicerow.getShort("system_type_id");
    		long devicestateTime = devicerow.getTimestamp("state_time").getTime();
    		
    		
    		short device_type = devicerow.getShort("device_type_id");
    		String latest_state = devicerow.getString("latest_state");
    		if (latest_state.equals("正常"))
    		{
    			devicestate_flag[0] = 1;
    			devicestate_flag[1] = 0;
    			devicestate_flag[2] = 0;
    		}
    		else if (latest_state.equals("异常"))
    		{
    			devicestate_flag[0] = 0;
    			devicestate_flag[1] = 1;
    			devicestate_flag[2] = 0;
    		}
    		else
    		{
    			devicestate_flag[0] = 0;
    			devicestate_flag[1] = 0;
    			devicestate_flag[2] = 1;
    		}
        	
        	//project
        	ResultSet projectCount = session.execute(String.format("SELECT update_time, normal_num, abnormal_num, offline_num FROM lateststatestat WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", projectUUID, system_type, device_type));
        	if (true == projectCount.isExhausted())
        	{
        		session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d  WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
						devicestateTime, devicestate_flag[0], devicestate_flag[1], devicestate_flag[2], projectUUID, system_type, device_type));
        	}
        	else
        	{
        		Row count_row = projectCount.one();
        		long updateTime = count_row.getTimestamp("update_time").getTime(); //可以用一个全局变量，每次更新的时候记录最新的时间
        		int normal_num = count_row.getInt("normal_num");
        		int abnormal_num = count_row.getInt("abnormal_num");
        		int offline_num = count_row.getInt("offline_num");
        		if (devicestateTime > updateTime)
        		{
        			updateTime = devicestateTime;

        		} 
    			session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
    					updateTime, normal_num + devicestate_flag[0], abnormal_num + devicestate_flag[1], offline_num + devicestate_flag[2], projectUUID, system_type, device_type));
        		
        	}
        	
        	//building
        	ResultSet buildingCount = session.execute(String.format("SELECT update_time, normal_num, abnormal_num, offline_num FROM lateststatestat WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", buildingUUID, system_type, device_type));
        	if (true == buildingCount.isExhausted())
        	{
        		session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d  WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
						devicestateTime, devicestate_flag[0], devicestate_flag[1], devicestate_flag[2], buildingUUID, system_type, device_type));
        	}
        	else
        	{
        		Row count_row = buildingCount.one();
        		long updateTime = count_row.getTimestamp("update_time").getTime(); //可以用一个全局变量，每次更新的时候记录最新的时间
        		int normal_num = count_row.getInt("normal_num");
        		int abnormal_num = count_row.getInt("abnormal_num");
        		int offline_num = count_row.getInt("offline_num");
        		if (devicestateTime > updateTime)
        		{
        			updateTime = devicestateTime;
        		}
    			session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
    					updateTime, normal_num + devicestate_flag[0], abnormal_num + devicestate_flag[1], offline_num + devicestate_flag[2], buildingUUID, system_type, device_type));
        		
        	}
        	
        	//floor
        	ResultSet floorCount = session.execute(String.format("SELECT update_time, normal_num, abnormal_num, offline_num FROM lateststatestat WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", floorUUID, system_type, device_type));
        	if (true == floorCount.isExhausted())
        	{
        		session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d  WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
						devicestateTime, devicestate_flag[0], devicestate_flag[1], devicestate_flag[2], floorUUID, system_type, device_type));
        	}
        	else
        	{
        		Row count_row = floorCount.one();
        		long updateTime = count_row.getTimestamp("update_time").getTime(); //可以用一个全局变量，每次更新的时候记录最新的时间
        		int normal_num = count_row.getInt("normal_num");
        		int abnormal_num = count_row.getInt("abnormal_num");
        		int offline_num = count_row.getInt("offline_num");
        		if (devicestateTime > updateTime)
        		{
        			updateTime = devicestateTime;
        		}
    			session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
    					updateTime, normal_num + devicestate_flag[0], abnormal_num + devicestate_flag[1], offline_num + devicestate_flag[2], floorUUID, system_type, device_type));
        		
        	}
        	
        	//system
        	ResultSet systemCount = session.execute(String.format("SELECT update_time, normal_num, abnormal_num, offline_num FROM lateststatestat WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", systemUUID, system_type, device_type));
        	if (true == systemCount.isExhausted())
        	{
        		session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d  WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
						devicestateTime, devicestate_flag[0], devicestate_flag[1], devicestate_flag[2], systemUUID, system_type, device_type));
        	}
        	else
        	{
        		Row count_row = systemCount.one();
        		long updateTime = count_row.getTimestamp("update_time").getTime(); //可以用一个全局变量，每次更新的时候记录最新的时间
        		int normal_num = count_row.getInt("normal_num");
        		int abnormal_num = count_row.getInt("abnormal_num");
        		int offline_num = count_row.getInt("offline_num");
        		if (devicestateTime > updateTime)
        		{
        			updateTime = devicestateTime;
        		}
    			session.execute(String.format("UPDATE lateststatestat SET update_time=%d, normal_num=%d, abnormal_num=%d, offline_num=%d WHERE item_id=%s AND system_type_id=%d AND device_type_id=%d;", 
    					updateTime, normal_num + devicestate_flag[0], abnormal_num + devicestate_flag[1], offline_num + devicestate_flag[2], systemUUID, system_type, device_type));
        		
        	}
        	
    	}
    }
        
    // 创建事件统计表    
    private void createEventStat()
    {
    	// 创建事件统计表，且urgent_num和general_num类型采用counter来计数
        session.execute(
                "CREATE TABLE IF NOT EXISTS eventstat (" +
                        "item_id uuid," +
                        "date timestamp," +
                        "system_type_id smallint," +
                        "device_type_id smallint," +
                        "event_type_id smallint," +
                        "urgent_num counter," +
                        "general_num counter," +
                        "PRIMARY KEY (item_id, date, system_type_id, device_type_id, event_type_id)" +
                        ") WITH CLUSTERING ORDER BY (date DESC);");
    	
    	int[] event_level_flag = new int[2];
    	HashMap<String, DeviceInfo> device_map = new HashMap<String, DeviceInfo>();	// 用于将设备信息提取到内存中，有利于后续更快的查找
    	ResultSet device_info = session.execute("SELECT * FROM devices;");
    	for (Row devicerow : device_info)
    	{
    		String projectUUID = devicerow.getUUID("project_id").toString();
    		String buildingUUID = devicerow.getUUID("building_id").toString();
    		String floorUUID = devicerow.getUUID("floor_id").toString();
    		String systemUUID = devicerow.getUUID("system_id").toString();
    		short device_type = devicerow.getShort("device_type_id");
    		short system_type = devicerow.getShort("system_type_id");
    		DeviceInfo device_msg = new DeviceInfo(projectUUID, buildingUUID, floorUUID, systemUUID, device_type, system_type);
    		
    		String deviceUUID = devicerow.getUUID("device_id").toString();
    		device_map.put(deviceUUID, device_msg);
    	}
    	   	
    	
    	ResultSet Events = session.execute("SELECT device_id, time, event_type_id, event_level FROM events;");
    	for (Row event_row : Events)
    	{
    		String deviceUUID = event_row.getUUID("device_id").toString();
    		long event_time = event_row.getTimestamp("time").getTime();
    		short event_type = event_row.getShort("event_type_id");
    		String event_level = event_row.getString("event_level");
    		
    		DeviceInfo device = device_map.get(deviceUUID);
    		short device_type = device.getdevicetype();
    		String systemUUID = device.getsystem();
    		String projectUUID = device.getproject();
    		String buildingUUID = device.getbuilding();
    		String floorUUID = device.getfloor();
    		short system_type = device.getsystemtype();
        	
        	if (event_level.equals("一般"))	// 如果是正常的事件则不做统计
        	{
        		event_level_flag[0] = 0;
        		event_level_flag[1] = 1;
        	}
        	else if (event_level.equals("紧急"))
        	{
        		event_level_flag[0] = 1;
        		event_level_flag[1] = 0;
        	}
        	else
        	{
        		continue;
        	}
        	
        	int i = 0;
        	for (; i < 1000; i++)  // i的最大值与eventstart_time和当前的时间有关系
        	{
        		if (event_time < (TestData.eventstart_time + ((i + 1) * 86400000L)) && event_time >= (TestData.eventstart_time + (i * 86400000L)))
        		{
        			break;
        		}
        	}
        	long date_time = TestData.eventstart_time + i * 86400000L;
        	
        	//project
        	session.execute(String.format("UPDATE eventstat SET urgent_num=urgent_num + %d, general_num=general_num + %d WHERE item_id=%s AND date=%d AND system_type_id=%d AND device_type_id=%d AND event_type_id=%d;", 
    				event_level_flag[0], event_level_flag[1], projectUUID, date_time, system_type, device_type, event_type)); 
        	
        	//building
        	session.execute(String.format("UPDATE eventstat SET urgent_num=urgent_num + %d, general_num=general_num + %d WHERE item_id=%s AND date=%d AND system_type_id=%d AND device_type_id=%d AND event_type_id=%d;", 
    				event_level_flag[0], event_level_flag[1], buildingUUID, date_time, system_type, device_type, event_type));
        	
        	//floor
        	session.execute(String.format("UPDATE eventstat SET urgent_num=urgent_num + %d, general_num=general_num + %d WHERE item_id=%s AND date=%d AND system_type_id=%d AND device_type_id=%d AND event_type_id=%d;", 
    				event_level_flag[0], event_level_flag[1], floorUUID, date_time, system_type, device_type, event_type));
        	
        	//system
        	session.execute(String.format("UPDATE eventstat SET urgent_num=urgent_num + %d, general_num=general_num + %d WHERE item_id=%s AND date=%d AND system_type_id=%d AND device_type_id=%d AND event_type_id=%d;", 
    				event_level_flag[0], event_level_flag[1], systemUUID, date_time, system_type, device_type, event_type));
        	
        	//device
        	session.execute(String.format("UPDATE eventstat SET urgent_num=urgent_num + %d, general_num=general_num + %d WHERE item_id=%s AND date=%d AND system_type_id=%d AND device_type_id=%d AND event_type_id=%d;", 
    				event_level_flag[0], event_level_flag[1], deviceUUID, date_time, system_type, device_type, event_type));
        	
    	}
    }
    
    
    
    private void CreateStaticTables() {
    	CreateStaticMetadata();
        SystemTypesTable();
        DeviceTypesTable();
        EventTypesTable();
        StateTypesTable();
        ParamTypesTable();
    }
    
    private void CreateStaticMetadata() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS devicetypes (" +
                        "device_type_id smallint," +
                        "device_type_name text," +
                        "PRIMARY KEY (device_type_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS eventtypes (" +
                        "event_type_id smallint," +
                        "event_level text," +
                        "event_type_name text," +
                        "PRIMARY KEY (event_type_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS paramtypes (" +
                        "param_type_id smallint," +
                        "param_type_name text," +
                        "param_unit text," +
                        "PRIMARY KEY (param_type_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS systemtypes (" +
                        "system_type_id smallint," +
                        "system_type_name text," +
                        "PRIMARY KEY (system_type_id)" +
                        ");");
        
        session.execute(
                "CREATE TABLE IF NOT EXISTS devicestatetypes (" +
                        "bit_pos tinyint," +
                        "state_val tinyint," +
                        "state_name text," +
                        "abnormal tinyint," +
                        "PRIMARY KEY (bit_pos, state_val)" +
                        ");");
    }
    
    private void SystemTypesTable()
    {
    	for (int i = 0; i < TestData.system_type_names.length; i++)
    	{
    		session.execute(String.format("INSERT INTO systemtypes (system_type_id, system_type_name) " +
						"VALUES (" + "%d," +"'%s'" +");", TestData.system_type_code[i], TestData.system_type_names[i]));
    	}
    	
    }
    
    private void DeviceTypesTable()
    {
    	for (int i = 0; i < TestData.device_code.length; i++)
    	{
    		session.execute(String.format("INSERT INTO devicetypes (device_type_id, device_type_name) " +
						"VALUES (" + "%d," +"'%s'" +");", TestData.device_code[i], TestData.device_names[i]));
    	}
    	
    }
    
    private void EventTypesTable()
    {
		session.execute("INSERT INTO eventtypes (event_type_id, event_level, event_type_name) " +
					"VALUES (" + "1," + "'正常'," +"'正常'" +");");
		
		for (int i = 0; i < TestData.urgent_code.length; i++)
		{
			session.execute(String.format("INSERT INTO eventtypes (event_type_id, event_level, event_type_name) " +
					"VALUES (" + "%d," + "'紧急'," + "'%s'" +");", TestData.urgent_code[i], TestData.urgent_event[i]));
		}
		
    	
    	for (int i = 0; i < TestData.common_code.length; i++)
    	{
    		session.execute(String.format("INSERT INTO eventtypes (event_type_id, event_level, event_type_name) " +
					"VALUES (" + "%d," + "'一般'," + "'%s'" +");", TestData.common_code[i], TestData.common_event[i]));
    	}
    	
    }
    
    private void StateTypesTable()
    {
    	
    	for (int i = 0; i < 11; i++)
    	{
    		for (int j = 0; j < 2; j++)
    		{
    			session.execute(String.format("INSERT INTO devicestatetypes (bit_pos, state_val, state_name, abnormal) " +
						"VALUES (" + "%d," + "%d," +"'%s'," + "%d" +");", i, j, TestData.state_names[i][j], j));
    		}
    	}
    	
    }
    
    private void ParamTypesTable()
    {
    	for (int i = 1; i < 18; i++)
    	{
    		session.execute(String.format("INSERT INTO paramtypes (param_type_id, param_type_name, param_unit) " +
						"VALUES (" + "%d," +"'%s'," +"'%s'" +");", i, TestData.param_names[i-1], TestData.param_unit[i-1]));
    	}
    	
    }
    
    
    public void close() {
        session.close();
        cluster.close();
    }
}
