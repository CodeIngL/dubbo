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
package com.alibaba.dubbo.rpc.proxy.wrapper;

import java.lang.reflect.Constructor;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.service.GenericService;

/**
 * StubProxyFactoryWrapper
 *
 * @author william.liangf
 */
public class StubProxyFactoryWrapper implements ProxyFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StubProxyFactoryWrapper.class);

    //被包装对象，默认为JavassistProxyFactory
    private final ProxyFactory proxyFactory;

    public StubProxyFactoryWrapper(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    //协议配置类
    private Protocol protocol;

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * 获得代理
     * 当消费引用暴露后，获得是一个代理，这里是最早的获得代理的调用处
     * 获得代理后，尝试对mock服务进行一次暴露，如果有的话。
     *
     * @param invoker rpc Invoker
     * @param <T>     类型
     * @return 代理
     * @throws RpcException rpc异常
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        T proxy = proxyFactory.getProxy(invoker);
        //不是泛化调用接口，
        if (GenericService.class != invoker.getInterface()) {
            //寻找mock服务，优先使用stub，stub等价于local
            String stub = invoker.getUrl().getParameter(Constants.STUB_KEY, invoker.getUrl().getParameter(Constants.LOCAL_KEY));
            if (ConfigUtils.isNotEmpty(stub)) {
                Class<?> serviceType = invoker.getInterface();
                if (ConfigUtils.isDefault(stub)) {
                    if (invoker.getUrl().hasParameter(Constants.STUB_KEY)) {
                        //mock服务类
                        stub = serviceType.getName() + "Stub";
                    } else {
                        //mock服务类
                        stub = serviceType.getName() + "Local";
                    }
                }
                try {
                    //检查mock服务类
                    Class<?> stubClass = ReflectUtils.forName(stub);
                    if (!serviceType.isAssignableFrom(stubClass)) {
                        throw new IllegalStateException("The stub implemention class " + stubClass.getName() + " not implement interface " + serviceType.getName());
                    }
                    try {
                        //完成mock服务的暴露
                        Constructor<?> constructor = ReflectUtils.findConstructor(stubClass, serviceType);
                        proxy = (T) constructor.newInstance(new Object[]{proxy});
                        URL url = invoker.getUrl();
                        if (url.getParameter(Constants.STUB_EVENT_KEY, Constants.DEFAULT_STUB_EVENT)) {
                            url = url.addParameter(Constants.STUB_EVENT_METHODS_KEY, StringUtils.join(Wrapper.getWrapper(proxy.getClass()).getDeclaredMethodNames(), ","));
                            url = url.addParameter(Constants.IS_SERVER_KEY, Boolean.FALSE.toString());
                            try {
                                export(proxy, (Class) invoker.getInterface(), url);
                            } catch (Exception e) {
                                LOGGER.error("export a stub service error.", e);
                            }
                        }
                    } catch (NoSuchMethodException e) {
                        throw new IllegalStateException("No such constructor \"public " + stubClass.getSimpleName() + "(" + serviceType.getName() + ")\" in stub implemention class " + stubClass.getName(), e);
                    }
                } catch (Throwable t) {
                    LOGGER.error("Failed to create stub implemention class " + stub + " in consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", cause: " + t.getMessage(), t);
                    // ignore
                }
            }
        }
        return proxy;
    }

    /**
     *
     * @param proxy 类型实例
     * @param type 类型类
     * @param url 元信息
     * @param <T>
     * @return
     * @throws RpcException
     */
    public <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) throws RpcException {
        return proxyFactory.getInvoker(proxy, type, url);
    }

    /**
     *
     * @param instance
     * @param type
     * @param url
     * @param <T>
     * @return
     */
    private <T> Exporter<T> export(T instance, Class<T> type, URL url) {
        return protocol.export(proxyFactory.getInvoker(instance, type, url));
    }

}