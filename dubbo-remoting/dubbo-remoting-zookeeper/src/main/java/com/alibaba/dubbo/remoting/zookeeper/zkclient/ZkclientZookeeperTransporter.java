package com.alibaba.dubbo.remoting.zookeeper.zkclient;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.zookeeper.ZookeeperClient;
import com.alibaba.dubbo.remoting.zookeeper.ZookeeperTransporter;

public class ZkclientZookeeperTransporter implements ZookeeperTransporter {

	/**
	 * 获得zk连接的客户端
	 * @param url 连接地址元信息
	 * @return dubbo对zk包装客户单
	 */
	public ZookeeperClient connect(URL url) {
		return new ZkclientZookeeperClient(url);
	}

}
