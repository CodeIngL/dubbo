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
package com.alibaba.dubbo.remoting.exchange;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.support.ExchangeHandlerDispatcher;
import com.alibaba.dubbo.remoting.exchange.support.Replier;
import com.alibaba.dubbo.remoting.transport.ChannelHandlerAdapter;

/**
 * Exchanger facade. (API, Static, ThreadSafe)(工具类)
 *
 * @author william.liangf
 */
public class Exchangers {

    private Exchangers() {
    }

    static {
        Version.checkDuplicate(Exchangers.class);
    }

    /**
     * 根据url元信息中的信息获得对应对应的交换器扩展类，默认是HeaderExchanger
     * url中的键为exchanger的值，默认是header,默认是HeaderExchanger
     *
     * @param url 元信息
     * @return Exchanger 交换器扩展类对应实例:HeaderExchanger实例
     */
    public static Exchanger getExchanger(URL url) {
        String type = url.getParameter(Constants.EXCHANGER_KEY, Constants.DEFAULT_EXCHANGER);
        return ExtensionLoader.getExtensionLoader(Exchanger.class).getExtension(type);
    }

    //======= 服务端bind相关 =======

    /**
     * @param url
     * @param replier
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(URL url, Replier<?> replier) throws RemotingException {
        return bind(url, new ChannelHandlerAdapter(), replier);
    }

    /**
     * @param url
     * @param handler
     * @param replier
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(URL url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return bind(url, new ExchangeHandlerDispatcher(replier, handler));
    }

    /**
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(String url, ExchangeHandler handler) throws RemotingException {
        return bind(URL.valueOf(url), handler);
    }

    /**
     * 绑定url和交换处理器，
     * 默认使用HeaderExchanger进行绑定
     *
     * @param url     元信息
     * @param handler 信息处理器
     * @return
     * @throws RemotingException
     */
    public static ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
        return getExchanger(url).bind(url, handler);
    }


    //======= 消费端connect相关 =======

    /**
     * @param url
     * @return
     * @throws RemotingException
     */
    public static ExchangeClient connect(String url) throws RemotingException {
        return connect(URL.valueOf(url));
    }

    /**
     * @param url
     * @return
     * @throws RemotingException
     */
    public static ExchangeClient connect(URL url) throws RemotingException {
        return connect(url, new ChannelHandlerAdapter(), null);
    }

    /**
     * 获得客户端
     *
     * @param url     元信息
     * @param replier
     * @return
     * @throws RemotingException
     * @see #connect(URL, ChannelHandler, Replier)
     */
    public static ExchangeClient connect(URL url, Replier<?> replier) throws RemotingException {
        return connect(url, new ChannelHandlerAdapter(), replier);
    }

    /**
     * 获得客户端
     *
     * @param url
     * @param handler
     * @param replier
     * @return
     * @throws RemotingException
     */
    public static ExchangeClient connect(URL url, ChannelHandler handler, Replier<?> replier) throws RemotingException {
        return connect(url, new ExchangeHandlerDispatcher(replier, handler));
    }

    /**
     * 获得客户端
     *
     * @param url
     * @param handler
     * @return
     * @throws RemotingException
     */
    public static ExchangeClient connect(String url, ExchangeHandler handler) throws RemotingException {
        return connect(URL.valueOf(url), handler);
    }

    /**
     * 获得客户端
     * <ul>
     * <li>入参检查，参数不得为空</li>
     * <li>添加解码编码器信息，默认对于dubbo协议来说，这一步忽略</li>
     * <li>获得相应的exchanger扩展类(HeaderExchanger)进行连接</li>
     * </ul>
     *
     * @param url     元信息
     * @param handler 处理器
     * @return 客户端实例 默认是HeaderExchangeClient实例对象
     * @throws RemotingException 远程异常
     */
    public static ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        url = url.addParameterIfAbsent(Constants.CODEC_KEY, "exchange");
        return getExchanger(url).connect(url, handler);
    }

}