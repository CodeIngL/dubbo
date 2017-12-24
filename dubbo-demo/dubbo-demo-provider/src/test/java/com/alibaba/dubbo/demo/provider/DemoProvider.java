/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.demo.provider;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.springframework.util.CollectionUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DemoProvider {

/*	public static void main(String[] args) {
        com.alibaba.dubbo.container.Main.main(args);
	}*/

	private ZkClient zkClient;

	private String masterNode;

	private String ip;

	private final String port = String.valueOf(RandomUtils.nextInt(new Random()));

	public void init(){
		try {
			if (zkClient == null) {
				zkClient = new ZkClient("127.0.0.1:2181", 3000, 3000, new SerializableSerializer());
				try {
					zkClient.createPersistent("/nbgl2master/master/da");
					//zkClient.createPersistent("/nbgl2master", InetAddress.getLocalHost().getHostAddress());

				}catch (Exception e){
					//ignore;
				}
				zkClient.createEphemeralSequential("/nbgl2master/master", InetAddress.getLocalHost().getHostAddress()+port);
				zkClient.subscribeChildChanges("/nbgl2master", new IZkChildListener() {
					public void handleChildChange(String s, List<String> list) throws Exception {
						if (CollectionUtils.isEmpty(list)){
							zkClient.createEphemeralSequential("/nbgl2master/master", InetAddress.getLocalHost().getHostAddress());
						}
					}
				});
			}
			List<String> children = zkClient.getChildren("/nbgl2master");
			if (!CollectionUtils.isEmpty(children)){
				Collections.sort(children);
				for (String str: children) {
					String data = zkClient.readData("/nbgl2master/"+str, true);
					masterNode = str;
					ip = data;
					System.out.println("遍历节点:"+ip+"   遍历masterNode:"+masterNode);
					if (StringUtils.isEmpty(ip)) {
						continue;
					}
					if (!ip.equals(InetAddress.getLocalHost().getHostAddress()+port)) {
						System.out.println("节点:"+ip+"   masterNode:"+masterNode);
						return;
					}
/*					if (!InetAddress.getLocalHost().getHostAddress().equals(ip)) {
						System.out.println("节点:"+ip+"   masterNode:"+masterNode);
						return;
					}*/
					break;
				}
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws InterruptedException {
/*		String ss = "/master/abc";
		String[] strings = ss.split("/");
		System.out.println(strings.length);
		for (String str: strings){
			System.out.println(str);
		}
		System.out.println("----------------");*/
		DemoProvider provider = new DemoProvider();
		for (;;){
			Thread.sleep(3000);
			provider.init();
		}


	}

}