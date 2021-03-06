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
package com.alibaba.dubbo.registry.integration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Configurator;
import com.alibaba.dubbo.rpc.protocol.InvokerWrapper;

import static com.alibaba.dubbo.common.Constants.*;
import static com.alibaba.dubbo.common.utils.StringUtils.isEmpty;

/**
 * RegistryProtocol
 *
 * @author william.liangf
 * @author chao.liuc
 */
public class RegistryProtocol implements Protocol {

    private Cluster cluster;

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    private Protocol protocol;

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    private RegistryFactory registryFactory;

    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    private ProxyFactory proxyFactory;

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    public int getDefaultPort() {
        return 9090;
    }

    private static RegistryProtocol INSTANCE;

    public RegistryProtocol() {
        INSTANCE = this;
    }

    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(Constants.REGISTRY_PROTOCOL); // load
        }
        return INSTANCE;
    }

    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<URL, NotifyListener>();

    public Map<URL, NotifyListener> getOverrideListeners() {
        return overrideListeners;
    }

    //用于解决rmi重复暴露端口冲突的问题，已经暴露过的服务不再重新暴露
    //providerurl <--> exporter
    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();

    private final static Logger logger = LoggerFactory.getLogger(RegistryProtocol.class);

    /**
     * <p>
     * 使用注册中心进行的暴露，除了暴露本地服务外还需要建立与注册中心的关系。
     * </p>
     *
     * @param originInvoker 默认情况下是AbstractProxyInvoker
     * @param <T>
     * @return
     * @throws RpcException rpc异常
     * @see #doLocalExport(Invoker)
     */
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {

        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker); //将本地的服务接口暴露为支持原创调用的接口
        final Registry registry = getRegistry(originInvoker);//获得配置的注册中心
        final URL registeredProviderUrl = getRegisteredProviderUrl(originInvoker);//获得需要注册进注册中心的元信息
        registry.register(registeredProviderUrl);//使用注册中心进行相关的注册

        // 订阅override数据
        // FIXME 提供者订阅时，会影响同一JVM即暴露服务，又引用同一服务的的场景，因为subscribed以服务名为缓存的key，导致订阅信息覆盖。
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registeredProviderUrl);//将注册中心上注册的url转换为需要被订阅的元信息

        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl); //为这个需要订阅的元信息构建相应的监听者

        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener); //放置缓存

        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);//对注册中心的相关url进行订阅，这个url可以通过overrideSubscribeUrl来导出

        //返回在注册中心下真正暴露在外的服务提供者。由Export进行包装。
        return new Exporter<T>() {
            public Invoker<T> getInvoker() {
                return exporter.getInvoker();
            }

            public void unexport() {
                try {
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
                try {
                    registry.unregister(registeredProviderUrl);
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
                try {
                    overrideListeners.remove(overrideSubscribeUrl);
                    registry.unsubscribe(overrideSubscribeUrl, overrideSubscribeListener);
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        };
    }

    /**
     * 尝试从缓存中直接取出
     * 没有则新建
     * <ul>
     * <li>使用原始的originInvoker(AbstractProxyInvoker类对象)，和其中得到的配置URL构建Invoker的委托实现</li><br/>
     * <li>使用Protocol$Adpative来导出委托Invoker，其中使用委托的URL（协议配置URL），来获得具体扩展类，使用具体扩展类导出委托的Invoker</li><br/>
     * <li>构建ExporterChangeableWrapper使用（导出的export和原始的originInvoker）</li><br/>
     * </ul>
     *
     * @param originInvoker 需要暴露的invoker
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker) {
        String key = getCacheKey(originInvoker);//key为需要导出的服务的url(provider的url,也就是注册url的export键对应值)去掉dynamic和enable键
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);//尝试从缓存中获得，防止重复暴露
        if (exporter == null) {
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                if (exporter == null) {
                    final Invoker<?> invokerDelegete = new InvokerDelegate<T>(originInvoker, getProviderUrl(originInvoker));//从invoker中获得相应的配置服务提供url
                    exporter = new ExporterChangeableWrapper<T>((Exporter<T>) protocol.export(invokerDelegete), originInvoker);
                    bounds.put(key, exporter);
                }
            }
        }
        return exporter;
    }

    /**
     * 对修改了url的invoker重新export
     *
     * @param originInvoker
     * @param newInvokerUrl
     */
    @SuppressWarnings("unchecked")
    private <T> void doChangeLocalExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        String key = getCacheKey(originInvoker);
        final ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            logger.warn(new IllegalStateException("error state, exporter should not be null"));
            return;//不存在是异常场景 直接返回
        } else {
            final Invoker<T> invokerDelegete = new InvokerDelegate<T>(originInvoker, newInvokerUrl);
            exporter.setExporter(protocol.export(invokerDelegete));
        }
    }

    /**
     * 根据invoker的信息获取registry实例
     * <p>invoker中携带了相关的url信息</p>
     * <p>对于注册中心来说其url的protocol总是registry，相比于其他的配置来说，
     * 但是，实际上的注册中心协议在其参数信息中的，键为registry所对应的信息，默认是dubbo，开发者一般会选择zookeeper</p>
     *
     * @param originInvoker 网络包装的调用者
     * @return 注册中心实例
     */
    private Registry getRegistry(final Invoker<?> originInvoker) {
        URL registryUrl = originInvoker.getUrl();//获得注册中心URL
        //进行转换，转换为实际的使用的注册中心协议，比如zookeeper，redis
        if (Constants.REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            String protocol = registryUrl.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_DIRECTORY);
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(Constants.REGISTRY_KEY);
        }
        //注册中心工厂通过实际的注册中心url来获得实际对应的注册中心对象
        return registryFactory.getRegistry(registryUrl);
    }

    /**
     * 返回注册到注册中心的URL，对URL参数进行一次过滤，移除以.开头的键，移除监控的键值信息
     *
     * @param originInvoker
     * @return
     */
    private URL getRegisteredProviderUrl(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        return providerUrl.removeParameters(getFilteredKeys(providerUrl)).removeParameter(Constants.MONITOR_KEY);//注册中心看到的地址
    }

    /**
     * 改变url的协议为provider
     * 添加键值对k:category v:configurators
     * 添加键值对k:check v:false
     *
     * @param registedProviderUrl 注册的服务提供者的URL
     * @return 订阅的URL
     */
    private URL getSubscribedOverrideUrl(URL registedProviderUrl) {
        return registedProviderUrl.setProtocol(PROVIDER_PROTOCOL)
                .addParameters(CATEGORY_KEY, CONFIGURATORS_CATEGORY, CHECK_KEY, String.valueOf(false));
    }

    /**
     * 通过invoker的url 获取 providerUrl的地址
     * 通过注册中心的url从其中取出key为export的值
     * 实际上就是配置协议的url
     *
     * @param originInvoker
     * @return
     */
    private URL getProviderUrl(final Invoker<?> originInvoker) {
        String providerUrl = originInvoker.getUrl().getParameterAndDecoded(Constants.EXPORT_KEY);
        if (isEmpty(providerUrl)) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + originInvoker.getUrl());
        }
        return URL.valueOf(providerUrl);
    }

    /**
     * 获取invoker在bounds中缓存的key
     * 首先从invoker中获得协议配置的url
     * 从中移除dynamic和enabled键值对，
     * 剩下的FullString得到字符串
     *
     * @param originInvoker
     * @return
     */
    private String getCacheKey(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        return providerUrl.removeParameters("dynamic", "enabled").toFullString();
    }

    /**
     * 消费方注册中心引用实现
     * <p>
     * <ul>
     * <li>第一步进行协议的转换说明，注册中心url的protocol（协议）一定是registry，真正的注册协议在其参数属性中，键为registry(消费端本身的信息在键为refer键中)</li><br/>
     * <li>第二步进行协议的转换过程，注册中心url重新设置其protocol为参数映射中registry对应的值，移除参数中registry的键值对</li></li><br/>
     * <li>通过转换过的注册中心协议获得相应的注册中心</li></li><br/>
     * <li>对应消费接口引用为RegistryService关于注册中心的直接返回，没有必要继续操作</li></li><br/>
     * <li>根据消费方的接口应用的参数来选取不同集群方式，对于多group的消费接口，使用MergeableCluster来集权调用,单个则使用默认值</li></li><br/>
     * </ul>
     * </p>
     *
     * @param type 服务的类型
     * @param url  远程服务的URL地址
     * @param <T>
     * @return 调用者
     * @throws RpcException rpc异常
     * @see #doRefer(Cluster, Registry, Class, URL)
     */
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        //协议转换回来，当protcol为registry只是临时代表这个需要注册到注册中心上，但是真正的协议类型还是元信息中的registry的值（默认的注册中心是dubbo注册中心）
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);
        //获得注册中心，特定注册中心由url的protocol(协议）决定。ex:zookeeper
        Registry registry = registryFactory.getRegistry(url);
        if (RegistryService.class.equals(type)) {
            //对于接口是RegistryService，直接获得Invoker后返回
            return proxyFactory.getInvoker((T) registry, type, url);
        }

        //处理group配置项:group="a,b" or group="*"
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        String group = qs.get(Constants.GROUP_KEY);
        if (group != null && group.length() > 0) {
            if ((Constants.COMMA_SPLIT_PATTERN.split(group)).length > 1
                    || "*".equals(group)) {
                //对于传递进来的接口引用，从属于多个组的，应使用MergeableCluster来聚集
                return doRefer(getMergeableCluster(), registry, type, url);
            }
        }
        //对于传递进来的接口引用，单个组的或者没有配置的，应使用默认的Cluster来聚集
        return doRefer(cluster, registry, type, url);
    }

    /**
     * @return MergeableCluster实例
     */
    private Cluster getMergeableCluster() {
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension("mergeable");
    }

    /**
     * <p>对于接口信息中group处理
     * <ul>
     * <li>group配置项，从属于多个组的(group=*;group=a,b)，cluster为MergeableCluster</li><br/>
     * <li>group配置项，属于单个组或者没有配置的(无group;group=a)，cluster为默认的cluster</li><br/>
     * </ul>
     * </p>
     * <p>
     * tip: 入参url--->其protocol为具体的注册中心协议，其refer为接口引用信息映射，不存在registry键。<br/>
     * tip: directory(注册目录服务)的getUrl返回的是overridedirctoryUrl，其参数信息就是消费方的参数信息，而不是注册中心url的参数信息，并且去掉了监控信息
     *
     * @param cluster  合并形式
     * @param registry 注册中心
     * @param type     接口类(不可能是RegistryService)
     * @param url      元信息，其protocol为具体的注册中心协议，其refer为接口引用信息映射，不存在registry键。
     * @param <T>      返回的Invoker
     * @return 返回的Invoker
     * @see Registry#register(URL)
     * @see RegistryDirectory#subscribe(URL)
     */
    private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // 注册中心目录服务
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        // 目录服务设置相应的注册中心
        directory.setRegistry(registry);
        // 目录服务设置相应的协议配置类（默认Protocol$Adaptive)
        directory.setProtocol(protocol);

        // 构建受订阅的url(consumer://本地地址:0/type?参数信息)。
        // 参数信息仅含接口引用的参数信息，同时去掉了监控信息
        // getUrl()实现上是overrideDirectoryUrl而不是url
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, NetUtils.getLocalHost(), 0, type.getName(), directory.getUrl().getParameters());

        if (!Constants.ANY_VALUE.equals(url.getServiceInterface()) && url.getParameter(Constants.REGISTER_KEY, true)) {
            // 非泛化调用，在注册中心相关目录下注册相关信息，目录为:consumers
            // 对于zk来说，会产生/dubbo/接口名/consumers/url的String；这样的路径，最后一个节点是否为临时节点，由url中的信息决定（key:dynamic)
            registry.register(subscribeUrl.addParameters(CATEGORY_KEY, Constants.CONSUMERS_CATEGORY, CHECK_KEY, String.valueOf(false)));
        }

        // 目录服务进行订阅(category:providers,configurators,routers)
        // 设置了目录服务的消费url，使用注册中心去订阅该消费url，
        // 目录服务订阅其需要消费的url的在注册中心上的相关信息
        directory.subscribe(subscribeUrl.addParameter(CATEGORY_KEY, Constants.PROVIDERS_CATEGORY + "," + CONFIGURATORS_CATEGORY + "," + Constants.ROUTERS_CATEGORY));
        return cluster.join(directory);
    }

    /**
     * 过滤URL中不需要输出的参数(以点号开头的)
     *
     * @param url 目标url
     * @return 需要过滤的键
     */
    private static String[] getFilteredKeys(URL url) {
        Map<String, String> params = url.getParameters();
        if (params == null || params.size() == 0) {
            return new String[]{};
        }
        List<String> filteredKeys = new ArrayList<String>();
        for (String key : params.keySet()) {
            if (key == null) {
                continue;
            }
            if (key.startsWith(HIDE_KEY_PREFIX)) {
                filteredKeys.add(key);
            }
        }
        return filteredKeys.toArray(new String[filteredKeys.size()]);
    }

    public void destroy() {
        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();
    }


    /*重新export 1.protocol中的exporter destory问题
     *1.要求registryprotocol返回的exporter可以正常destroy
     *2.notify后不需要重新向注册中心注册
     *3.export 方法传入的invoker最好能一直作为exporter的invoker.
     */
    private class OverrideListener implements NotifyListener {

        private volatile List<Configurator> configurators;

        //监听者所订阅消费的url
        private final URL subscribeUrl;

        public OverrideListener(URL subscribeUrl) {
            this.subscribeUrl = subscribeUrl;
        }

        /*
         *  provider 端可识别的override url只有这两种.
         *  override://0.0.0.0/serviceName?timeout=10
         *  override://0.0.0.0/?timeout=10
         */
        public void notify(List<URL> urls) {
            List<URL> result = null;
            for (URL url : urls) {
                URL overrideUrl = url;
                if (url.getParameter(CATEGORY_KEY) == null && Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    // 兼容旧版本
                    overrideUrl = url.addParameter(CATEGORY_KEY, CONFIGURATORS_CATEGORY);
                }
                if (!UrlUtils.isMatch(subscribeUrl, overrideUrl)) {
                    if (result == null) {
                        result = new ArrayList<URL>(urls);
                    }
                    result.remove(url);
                    logger.warn("Subsribe category=configurator, but notifed non-configurator urls. may be registry bug. unexcepted url: " + url);
                }
            }
            if (result != null) {
                urls = result;
            }
            this.configurators = RegistryDirectory.toConfigurators(urls);
            List<ExporterChangeableWrapper<?>> exporters = new ArrayList<ExporterChangeableWrapper<?>>(bounds.values());
            for (ExporterChangeableWrapper<?> exporter : exporters) {
                Invoker<?> invoker = exporter.getOriginInvoker();
                final Invoker<?> originInvoker;
                if (invoker instanceof InvokerDelegate) {
                    originInvoker = ((InvokerDelegate<?>) invoker).getInvoker();
                } else {
                    originInvoker = invoker;
                }

                URL originUrl = RegistryProtocol.this.getProviderUrl(originInvoker);
                URL newUrl = getNewInvokerUrl(originUrl, urls);

                if (!originUrl.equals(newUrl)) {
                    RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                }
            }
        }

        private URL getNewInvokerUrl(URL url, List<URL> urls) {
            List<Configurator> localConfigurators = this.configurators; // local reference
            // 合并override参数
            if (localConfigurators != null && localConfigurators.size() > 0) {
                for (Configurator configurator : localConfigurators) {
                    url = configurator.configure(url);
                }
            }
            return url;
        }
    }

    /**
     * @param <T>
     */
    public static class InvokerDelegate<T> extends InvokerWrapper<T> {

        private final Invoker<T> invoker;

        /**
         * @param invoker
         * @param url     invoker.getUrl返回此值
         */
        public InvokerDelegate(Invoker<T> invoker, URL url) {
            super(invoker, url);
            this.invoker = invoker;
        }

        /**
         * @return
         */
        public Invoker<T> getInvoker() {
            if (invoker instanceof InvokerDelegate) {
                return ((InvokerDelegate<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    /**
     * exporter代理,建立返回的exporter与protocol export出的exporter的对应关系，在override时可以进行关系修改.
     *
     * @param <T>
     * @author chao.liuc
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        private Exporter<T> exporter;

        private final Invoker<T> originInvoker;

        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }

        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        public void unexport() {
            String key = getCacheKey(this.originInvoker);
            bounds.remove(key);
            exporter.unexport();
        }
    }
}