package com.cassandra.data.CreateData;

/**
 * 单个设备的信息列表及提取
 * @author zhangmalin
 *
 */

public final class DeviceInfo {
	private final String projectUUID;
	private final String buildingUUID;
	private final String floorUUID;
	private final String systemUUID;
	private final short devicetypeId;
	private final short systemtypeId;
	
	public DeviceInfo(String project_id, 
					String building_id,
					String floor_id,
					String system_id,
					short device_type,
					short system_type) {
		projectUUID = project_id;
		buildingUUID = building_id;
		floorUUID = floor_id;
		systemUUID = system_id;
		devicetypeId = device_type;
		systemtypeId = system_type;
		
	}
	
	public String getproject()
	{
		return projectUUID;
	}
	
	public String getbuilding()
	{
		return buildingUUID;
	}
	
	public String getfloor()
	{
		return floorUUID;
	}
	
	public String getsystem()
	{
		return systemUUID;
	}

	public short getdevicetype()
	{
		return devicetypeId;
	}
	
	public short getsystemtype()
	{
		return systemtypeId;
	}
}
