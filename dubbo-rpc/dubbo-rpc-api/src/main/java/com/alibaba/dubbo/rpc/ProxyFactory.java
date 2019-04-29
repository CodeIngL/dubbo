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
package com.alibaba.dubbo.rpc;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Adaptive;
import com.alibaba.dubbo.common.extension.SPI;

/**
 * ProxyFactory. (API/SPI, Singleton, ThreadSafe)
 * 
 * @author william.liangf
 */
@SPI("javassist")
public interface ProxyFactory {

    /**
     * <p>
     *     通过Invoker构建一个代理
     * </p>
     * <p>
     *     代理负责对远程调用的透明化
     * </p>
     *
     * @param invoker rpc Invoker
     * @param <T> 类型
     * @return 代理
     * @throws RpcException rpc异常
     */
    @Adaptive({Constants.PROXY_KEY})
    <T> T getProxy(Invoker<T> invoker) throws RpcException;

    /**
     *
     * 创建 Invoker.
     * @param <T> Invoker的类型
     * @param proxy 类型实例
     * @param type 类型类
     * @param url 元信息
     * @return invoker
     */
    @Adaptive({Constants.PROXY_KEY})
    <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) throws RpcException;

}