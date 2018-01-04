package com.cassandra.data.CreateData;

/**
 * 生成数据库所需模拟数据的入口主程序
 * @author zhangmalin
 * @version 2.0.0
 */

public class App 
{
    public static void main(String[] args)
    {
        System.out.println( "Hello World!" );
//        String[] IP = {"10.20.47.187"};
//        int Port = 32779;
        CassandraDemo cass_client = new CassandraDemo();
//        CassandraOld cass_old = new CassandraOld();
        try {

          cass_client.connect(CassandraProperties.CONTACT_POINTS, CassandraProperties.PORT);	// 与数据库建立连接
//        	cass_old.connect(IP, Port);
//          cass_client.BuildData();
//          cass_client.BuildStat();
//          cass_client.BuildDeviceMap();
//          cass_client.DeleteTables();
//          cass_client.UserManage();
//          cass_client.ChangeTables();
//          cass_client.AddMaintainers();
          cass_client.createIndex();
//        	cass_old.CreateUserTable();
//        	cass_old.DeleteTable();
      } finally {
          cass_client.close();
//    	    cass_old.close();
      }
      
    }
}
