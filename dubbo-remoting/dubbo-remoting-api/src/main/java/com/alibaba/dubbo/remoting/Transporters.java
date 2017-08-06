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
package com.alibaba.dubbo.remoting;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.remoting.transport.ChannelHandlerAdapter;
import com.alibaba.dubbo.remoting.transport.ChannelHandlerDispatcher;

/**
 * Transporter facade. (API, Static, ThreadSafe)
 *
 * @author william.liangf
 */
public class Transporters {

    private Transporters() {
    }

    public static final Transporter transporter = ExtensionLoader.getExtensionLoader(Transporter.class).getAdaptiveExtension();

    //======= 服务端bind相关 =======

    /**
     * @param url      元信息
     * @param handlers 通道处理器
     * @return 服务端实例
     * @throws RemotingException 异常信息
     * @see #bind(URL, ChannelHandler...)
     */
    public static Server bind(String url, ChannelHandler... handlers) throws RemotingException {
        return bind(URL.valueOf(url), handlers);
    }

    /**
     * 传输对象，绑定url和处理器来获得网络服务对象
     * <ul>
     * <li>对入参进行检查</li><br/>
     * <li>对多个处理器，使用ChannelHandlerDispatcher封装</li><br/>
     * <li>使用特定的传输对象来绑定，传输对象决定来自url元信息，默认是netty传输对象</li><br/>
     * </ul>
     *
     * @param url      元信息
     * @param handlers 通道处理器
     * @return 服务端实例
     * @throws RemotingException 异常信息
     */
    public static Server bind(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("handlers == null");
        }
        ChannelHandler handler;
        if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            handler = new ChannelHandlerDispatcher(handlers);
        }
        //Transporter$Adpative导出，默认总是netty
        return transporter.bind(url, handler);
    }

    //======= 消费端connect相关 =======

    /**
     * @param url      元信息
     * @param handlers 通道处理器
     * @return 客户端实例
     * @throws RemotingException 异常信息
     * @see #connect(URL, ChannelHandler...)
     */
    public static Client connect(String url, ChannelHandler... handlers) throws RemotingException {
        return connect(URL.valueOf(url), handlers);
    }

    /**
     * 获得客户端
     *
     * @param url      元信息
     * @param handlers 通道处理器
     * @return 客户端实例
     * @throws RemotingException 异常信息
     */
    public static Client connect(URL url, ChannelHandler... handlers) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        ChannelHandler handler;
        if (handlers == null || handlers.length == 0) {
            handler = new ChannelHandlerAdapter();
        } else if (handlers.length == 1) {
            handler = handlers[0];
        } else {
            handler = new ChannelHandlerDispatcher(handlers);
        }
        return transporter.connect(url, handler);
    }

}