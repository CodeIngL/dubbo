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
package com.alibaba.dubbo.remoting.transport.netty;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Client;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.Server;
import com.alibaba.dubbo.remoting.Transporter;

/**
 * netty传输实现
 * @author ding.lid
 */
public class NettyTransporter implements Transporter {

    public static final String NAME = "netty";

    /**
     * 获得server
     * <ul>
     *     <li>简单生成netty服务服务端对象</li><br/>
     * </ul>
     * @param url 元信息
     * @param listener 处理器
     * @return 服务端实例
     * @throws RemotingException 异常信息
     */
    public Server bind(URL url, ChannelHandler listener) throws RemotingException {
        return new NettyServer(url, listener);
    }

    /**
     * 获得client
     * <ul>
     *     <li>简单生成netty客户端对象</li>
     * </ul>
     * @param url 元信息
     * @param listener 处理器
     * @return 客户端实例
     * @throws RemotingException 异常信息
     */
    public Client connect(URL url, ChannelHandler listener) throws RemotingException {
        return new NettyClient(url, listener);
    }

}