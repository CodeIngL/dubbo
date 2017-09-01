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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.Router;
import com.alibaba.dubbo.rpc.cluster.RouterFactory;
import com.alibaba.dubbo.rpc.cluster.router.MockInvokersSelector;

/**
 * 增加router的Directory
 *
 * @author chao.liuc
 */
public abstract class AbstractDirectory<T> implements Directory<T> {

    // 日志输出
    private static final Logger logger = LoggerFactory.getLogger(AbstractDirectory.class);

    // 核心完整url，有很多信息
    private final URL url;

    private volatile boolean destroyed = false;

    //目录服务所需要进行消费的url
    private volatile URL consumerUrl;

    //路由列表
    private volatile List<Router> routers;

    /**
     * 抽象类的目录服务构造方法<br/>
     * 不带上路由,会自动生成一个长度为1的元素为MockInvokersSelector的路由列表
     *
     * @param url 元信息
     * @see AbstractDirectory#AbstractDirectory(URL, List)
     */
    public AbstractDirectory(URL url) {
        this(url, null);
    }

    /**
     * 抽象类的目录服务构造方法<br/>
     * 默认主体的url和消费的url一致<br/>
     *
     * @param url 元信息
     * @see AbstractDirectory#AbstractDirectory(URL, URL, List)
     */
    public AbstractDirectory(URL url, List<Router> routers) {
        this(url, url, routers);
    }

    /**
     * 抽象类的目录服务构造方法<br/>
     * <p>
     * <ul>设置目录主体的url</ul><br/>
     * <ul>设置目录消费的url</ul><br/>
     * <ul>设置路由信息</ul><br/>
     * </p>
     *
     * @param url         元信息
     * @param consumerUrl 元信息
     * @param routers     路由列表
     * @see #setRouters(List)
     */
    public AbstractDirectory(URL url, URL consumerUrl, List<Router> routers) {
        if (url == null)
            throw new IllegalArgumentException("url == null");
        this.url = url;
        this.consumerUrl = consumerUrl;
        setRouters(routers);
    }

    /**
     *
     * 目录服务根据调用对象寻找与之对应的调用者列表
     *
     * @param invocation 调用对象
     * @return 调用者列表
     * @throws RpcException rpc异常
     */
    public List<Invoker<T>> list(Invocation invocation) throws RpcException {
        //检查合法性
        if (destroyed) {
            throw new RpcException("Directory already destroyed .url: " + getUrl());
        }

        //回调子类实现
        List<Invoker<T>> invokers = doList(invocation);

        //目录服务存相应的路由，则使用路由完成
        List<Router> localRouters = this.routers; // local reference
        if (localRouters != null && localRouters.size() > 0) {
            for (Router router : localRouters) {
                try {
                    if (router.getUrl() == null || router.getUrl().getParameter(Constants.RUNTIME_KEY, true)) {
                        invokers = router.route(invokers, getConsumerUrl(), invocation);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
                }
            }
        }
        return invokers;
    }

    /**
     * 注意子类的不同实现
     * @return 元信息
     */
    public URL getUrl() {
        return url;
    }

    public List<Router> getRouters() {
        return routers;
    }

    public URL getConsumerUrl() {
        return consumerUrl;
    }

    public void setConsumerUrl(URL consumerUrl) {
        this.consumerUrl = consumerUrl;
    }

    /**
     * <p>
     * 路由信息的设置,路由信息来自目录服务持有的主体url
     * <ul>
     * <li>从url中获得路由信息</li></li><br/>
     * <li>根据路由信息加载不同的路由扩展类，并加入路由列表</li><br/>
     * <li>增加一个mock的路由</li><br/>
     * <li>排序，并设置上面的路由列表</li><br/>
     * </ul>
     * </p>
     *
     * @param routers 路由列表
     */
    protected void setRouters(List<Router> routers) {
        // copy list
        routers = routers == null ? new ArrayList<Router>() : new ArrayList<Router>(routers);
        //获得url中的路由信息，并加入路由列表中
        String routerkey = url.getParameter(Constants.ROUTER_KEY);
        if (routerkey != null && routerkey.length() > 0) {
            RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getExtension(routerkey);
            routers.add(routerFactory.getRouter(url));
        }
        //增加一个mock选择器
        routers.add(new MockInvokersSelector());
        //排序
        Collections.sort(routers);
        this.routers = routers;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    public void destroy() {
        destroyed = true;
    }

    protected abstract List<Invoker<T>> doList(Invocation invocation) throws RpcException;

}