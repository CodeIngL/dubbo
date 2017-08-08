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
package com.alibaba.dubbo.rpc.proxy;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.service.EchoService;

/**
 * AbstractProxyFactory
 *
 * @author william.liangf
 */
public abstract class AbstractProxyFactory implements ProxyFactory {

    /**
     * JavassistProxyFactory 和 JdkProxyFactory的共有实现
     * @param invoker rpc Invoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        Class<?>[] interfaces = null;
        String config = invoker.getUrl().getParameter("interfaces");
        //there exist bug ,null point
        if (config != null && config.length() > 0) {
            String[] types = Constants.COMMA_SPLIT_PATTERN.split(config);
            interfaces = new Class<?>[types.length + 1];
            interfaces[0] = invoker.getInterface();
            interfaces[1] = EchoService.class;
            for (int i = 0; i < types.length; i++) {
                interfaces[i + 1] = ReflectUtils.forName(types[i]);
            }
        }
        if (interfaces == null) {
            interfaces = new Class<?>[]{invoker.getInterface(), EchoService.class};
        }
        return getProxy(invoker, interfaces);
    }

    /**
     * 获得代理(子类实现)
     * @param invoker invoker
     * @param types 接口数组
     * @param <T> 代理类型
     * @return 代理实例
     * @see com.alibaba.dubbo.rpc.proxy.javassist.JavassistProxyFactory#getProxy(Invoker, Class[])
     * @see com.alibaba.dubbo.rpc.proxy.jdk.JdkProxyFactory#getProxy(Invoker, Class[])
     */
    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);

}