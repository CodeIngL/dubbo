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
package com.alibaba.dubbo.remoting.transport.dispatcher;


import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Dispatcher;
import com.alibaba.dubbo.remoting.exchange.support.header.HeartbeatHandler;
import com.alibaba.dubbo.remoting.transport.MultiMessageHandler;

/**
 * @author chao.liuc
 *
 */
public class ChannelHandlers {

    /**
     * 形成handler代理
     * @param handler 将传入参数AllChannelHandler实例
     * @param url 元信息
     * @return 封装的handler(MultiMessageHandler实例)
     * @see ChannelHandlers#wrapInternal(ChannelHandler, URL)
     */
    public static ChannelHandler wrap(ChannelHandler handler, URL url){
        return ChannelHandlers.getInstance().wrapInternal(handler, url);
    }

    protected ChannelHandlers() {}

    /**
     * 封装相应的handler形成委托。
     * <ul>
     *     <li>获得Dispatcher的扩展类，默认最终使用AllDispatcher</li><br/>
     *     <li>将传入参数handler包装成AllChannelHandler实例</li><br/>
     *     <li>将传入参数AllChannelHandler实例包装成HeartbeatHandler</li><br/>
     *     <li>将传入参数AllChannelHandler实例包装成MultiMessageHandler</li><br/>
     * </ul>
     * @param handler
     * @param url 元信息
     * @return 封装的handler
     */
    protected ChannelHandler wrapInternal(ChannelHandler handler, URL url) {
        return new MultiMessageHandler(new HeartbeatHandler(ExtensionLoader.getExtensionLoader(Dispatcher.class)
                                        .getAdaptiveExtension().dispatch(handler, url)));
    }

    private static ChannelHandlers INSTANCE = new ChannelHandlers();

    protected static ChannelHandlers getInstance() {
        return INSTANCE;
    }

    static void setTestingChannelHandlers(ChannelHandlers instance) {
        INSTANCE = instance;
    }
}