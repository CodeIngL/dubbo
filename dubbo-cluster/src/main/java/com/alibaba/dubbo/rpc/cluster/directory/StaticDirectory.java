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
package com.alibaba.dubbo.rpc.cluster.directory;

import java.util.List;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Router;

/**
 * StaticDirectory(静态目录服务)
 *
 * @author william.liangf
 */
public class StaticDirectory<T> extends AbstractDirectory<T> {

    //持有的invoker列表
    private final List<Invoker<T>> invokers;

    /**
     *
     * @param invokers
     */
    public StaticDirectory(List<Invoker<T>> invokers) {
        this(null, invokers, null);
    }

    /**
     *
     * @param invokers
     * @param routers
     */
    public StaticDirectory(List<Invoker<T>> invokers, List<Router> routers) {
        this(null, invokers, routers);
    }

    /**
     *
     * @param url
     * @param invokers
     */
    public StaticDirectory(URL url, List<Invoker<T>> invokers) {
        this(url, invokers, null);
    }

    /**
     *
     * @param url 注册中心的url(带上集群调用策略)
     * @param invokers 多个invoker
     * @param routers 路由集合
     */
    public StaticDirectory(URL url, List<Invoker<T>> invokers, List<Router> routers) {
        //自带注册中心的url为空的情况下使用invoker中的第一个url作为url
        super(url == null && invokers != null && invokers.size() > 0 ? invokers.get(0).getUrl() : url, routers);
        //invoker非空抛错
        if (invokers == null || invokers.size() == 0)
            throw new IllegalArgumentException("invokers == null");
        this.invokers = invokers;
    }

    public Class<T> getInterface() {
        return invokers.get(0).getInterface();
    }

    /**
     * invoker中第一个可用就放回true
     * @return 是否可用
     */
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        for (Invoker<T> invoker : invokers) {
            if (invoker.isAvailable()) {
                return true;
            }
        }
        return false;
    }

    public void destroy() {
        if (isDestroyed()) {
            return;
        }
        super.destroy();
        for (Invoker<T> invoker : invokers) {
            invoker.destroy();
        }
        invokers.clear();
    }

    @Override
    protected List<Invoker<T>> doList(Invocation invocation) throws RpcException {
        return invokers;
    }

}