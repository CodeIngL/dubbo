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
package com.alibaba.dubbo.remoting.exchange.support.header;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.Transporters;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.ExchangeHandler;
import com.alibaba.dubbo.remoting.exchange.ExchangeServer;
import com.alibaba.dubbo.remoting.exchange.Exchanger;
import com.alibaba.dubbo.remoting.transport.DecodeHandler;

/**
 * DefaultMessenger（默认的信使）
 *
 * @author william.liangf
 */
public class HeaderExchanger implements Exchanger {

    public static final String NAME = "header";

    /**
     * 直接返回HeaderExchangeServer的对象
     * <p>
     * 处理器（handler）会被多次包装
     * <ul>
     * <li>HeaderExchangeHandler包装</li><br/>
     * <li>DecodeHandler包装</li><br/>
     * <li>connect内部会尝试再次包装</li><br/>
     * </ul>
     * 包装结果会得到一个网络框架相关的server，然后被HeaderExchangeServer包装
     * 最后返回HeaderExchangeServer
     * </p>
     *
     * @param url     元信息
     * @param handler 原始的handler(被包装的handler)
     * @return 网络服务端
     * @throws RemotingException 异常信息
     * @see Transporters#bind(String, ChannelHandler...)
     */
    public ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        return new HeaderExchangeServer(Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
    }

    /**
     * 直接返回HeaderExchangeClient的对象
     * <p>
     * 处理器（handler）会被多次包装
     * <ul>
     * <li>HeaderExchangeHandler包装</li><br/>
     * <li>DecodeHandler包装</li><br/>
     * <li>connect内部会尝试再次包装</li><br/>
     * </ul>
     * 包装结果会得到一个网络框架相关的client，然后被HeaderExchangeClient包装
     * 最后返回HeaderExchangeClient
     * </p>
     *
     * @param url     元信息
     * @param handler 原始的handler(被包装的handler)
     * @return 网络客户端
     * @throws RemotingException 异常信息
     * @see Transporters#connect(String, ChannelHandler...)
     */
    public ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        return new HeaderExchangeClient(Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
    }

}