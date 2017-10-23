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
package com.alibaba.dubbo.registry.dubbo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.registry.integration.RegistryDirectory;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.cluster.Cluster;

/**
 * DubboRegistryFactory
 *
 * @author william.liangf
 */
public class DubboRegistryFactory extends AbstractRegistryFactory {

    private Protocol protocol;

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    private ProxyFactory proxyFactory;

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    private Cluster cluster;

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * 注册中心配置为dubbo的实现，这里涉及到多个地址，比如url中有备份地址
     * <ul>
     * <li>获得稳定的注册地址{@link #getRegistryURL(URL)}</li>
     * </ul></>
     *
     * @param url
     * @return
     */
    public Registry createRegistry(URL url) {
        //获得注册中心的url
        url = getRegistryURL(url);
        List<URL> urls = new ArrayList<URL>();
        //添加主url
        urls.add(url.removeParameter(Constants.BACKUP_KEY));
        //添加备份url
        String backup = url.getParameter(Constants.BACKUP_KEY);
        if (backup != null && backup.length() > 0) {
            String[] addresses = Constants.COMMA_SPLIT_PATTERN.split(backup);
            for (String address : addresses) {
                urls.add(url.setAddress(address));
            }
        }
        //为url构建注册中心目录服务
        RegistryDirectory<RegistryService> directory = new RegistryDirectory<RegistryService>(RegistryService.class,
                url.addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName()).addParameterAndEncoded(Constants.REFER_KEY, url.toParameterString()));
        Invoker<RegistryService> registryInvoker = cluster.join(directory);
        RegistryService registryService = proxyFactory.getProxy(registryInvoker);
        DubboRegistry registry = new DubboRegistry(registryInvoker, registryService);
        directory.setRegistry(registry);
        directory.setProtocol(protocol);
        directory.notify(urls);
        directory.subscribe(new URL(Constants.CONSUMER_PROTOCOL, NetUtils.getLocalHost(), 0, RegistryService.class.getName(), url.getParameters()));
        return registry;
    }

    /**
     * 获得稳定的地址信息(元信息中不变的部分)
     * <ul>
     * <li>设置path:com.alibaba.dubbo.registry。RegistryService</li>
     * <li>移除相关的注册信息，对于服务端是export属性，对于消费端是refer属性</li>
     * <li>添加（interfece:com.alibaba.dubbo.registry。RegistryService）</li>
     * <li>添加（sticky:true）</li>
     * <li>添加（lazy:true）</li>
     * <li>添加（reconnect:false）</li>
     * <li>添加（timeout:10000）</li>
     * <li>添加（callbacks:10000）</li>
     * <li>添加（connect.timeout:10000）</li>
     * <li>添加（methods:RegistryService中的相关方法）</li>
     * <li>添加（subscribe.1.callback:true）</li>
     * <li>添加（unsubscribe.1.callback:false）</li>
     * </ul>
     *
     * @param url 元信息
     * @return 新元信息
     */
    private static URL getRegistryURL(URL url) {
        return url.setPath(RegistryService.class.getName())
                .removeParameter(Constants.EXPORT_KEY).removeParameter(Constants.REFER_KEY)
                .addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName())
                //
                .addParameter(Constants.CLUSTER_STICKY_KEY, "true")
                .addParameter(Constants.LAZY_CONNECT_KEY, "true")
                .addParameter(Constants.RECONNECT_KEY, "false")
                .addParameterIfAbsent(Constants.TIMEOUT_KEY, "10000")
                .addParameterIfAbsent(Constants.CALLBACK_INSTANCES_LIMIT_KEY, "10000")
                .addParameterIfAbsent(Constants.CONNECT_TIMEOUT_KEY, "10000")
                .addParameter(Constants.METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(Wrapper.getWrapper(RegistryService.class).getDeclaredMethodNames())), ","))
                //.addParameter(Constants.STUB_KEY, RegistryServiceStub.class.getName())
                //.addParameter(Constants.STUB_EVENT_KEY, Boolean.TRUE.toString()) //for event dispatch
                //.addParameter(Constants.ON_DISCONNECT_KEY, "disconnect")
                .addParameter("subscribe.1.callback", "true")
                .addParameter("unsubscribe.1.callback", "false");
    }
}