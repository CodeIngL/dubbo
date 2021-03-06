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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Configurator;
import com.alibaba.dubbo.rpc.cluster.ConfiguratorFactory;
import com.alibaba.dubbo.rpc.cluster.Router;
import com.alibaba.dubbo.rpc.cluster.RouterFactory;
import com.alibaba.dubbo.rpc.cluster.directory.AbstractDirectory;
import com.alibaba.dubbo.rpc.cluster.directory.StaticDirectory;
import com.alibaba.dubbo.rpc.cluster.support.ClusterUtils;
import com.alibaba.dubbo.rpc.protocol.InvokerWrapper;
import com.alibaba.dubbo.rpc.support.RpcUtils;

/**
 * RegistryDirectory(注册中心目录服务)
 *
 * @author william.liangf
 * @author chao.liuc
 */
public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    //Cluster$Adaptive单例唯一,
    //加载时刻，类载入jvm后
    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    //RouterFactory$Adaptive单例唯一,
    //加载时刻，类载入jvm后
    private static final RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

    //ConfiguratorFactory$Adaptive单例唯一,
    //加载时刻，类载入jvm后
    private static final ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getAdaptiveExtension();

    //协议配置
    private Protocol protocol; // 注入时初始化，断言不为null

    //持有的注册中心
    private Registry registry; // 注入时初始化，断言不为null

    //接口引用的服务标识（serviceGroup+"/"+serviceName+":"serviceVersion）
    private final String serviceKey; // 构造时初始化，断言不为null

    //持有的接口引用类型
    private final Class<T> serviceType; // 构造时初始化，断言不为null

    //接口引用方信息（注册中心url的refer键)
    private final Map<String, String> queryMap; // 构造时初始化，断言不为null

    //目录url(对应可覆盖的目录url)
    //默认情况下等价于注册中心url去掉其本身相关参数，并添加接口引用的相关参数,并去掉接口引用参数中的关于监控的信息，同时其path为RegistryService的全类名
    private final URL directoryUrl; // 构造时初始化，断言不为null，并且总是赋非null值

    //可覆盖的目录url
    private volatile URL overrideDirectoryUrl; // 构造时初始化，断言不为null，并且总是赋非null值

    //接口引用所要rpc的方法名的列表
    private final String[] serviceMethods;

    //接口引用属于多个group的标志，当group配置项为*或者a,b形式
    private final boolean multiGroup;

    private volatile boolean forbidden = false;

    /*override规则 
     * 优先级：override>-D>consumer>provider
     * 第一种规则：针对某个provider <ip:port,timeout=100>
     * 第二种规则：针对所有provider <* ,timeout=5000>
     */
    private volatile List<Configurator> configurators; // 初始为null以及中途可能被赋为null，请使用局部变量引用

    // Map<url, Invoker> cache service url to invoker mapping.
    // url级别的invoker映射，key为url的fullString，同时该url是合并的url集合多种形式的参数
    private volatile Map<String, Invoker<T>> urlInvokerMap; // 初始为null以及中途可能被赋为null，请使用局部变量引用

    // Map<methodName, Invoker> cache service method to invokers mapping.
    // 方法级别的invoker映射
    private volatile Map<String, List<Invoker<T>>> methodInvokerMap; // 初始为null以及中途可能被赋为null，请使用局部变量引用

    // Set<invokerUrls> cache invokeUrls to invokers mapping.
    private volatile Set<URL> cachedInvokerUrls; // 初始为null以及中途可能被赋为null，请使用局部变量引用

    /**
     * 构建注册中心的目录服务实例
     *
     * @param serviceType 注册服务类
     * @param url         元信息，其protocol为具体的注册中心协议，其refer为接口引用信息映射，不存在registry键。
     */
    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(url);
        //检验入参
        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }
        //检验入参
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }
        this.serviceType = serviceType;
        this.serviceKey = url.getServiceKey();
        this.queryMap = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        //设置目录url，浅拷贝:为入参url去掉注册中心url的相关参数，并添加接口引用的相关参数,并去掉接口引用参数中的关于监控的信息
        this.overrideDirectoryUrl = this.directoryUrl = url.setPath(url.getServiceInterface()).clearParameters().addParameters(queryMap).removeParameter(Constants.MONITOR_KEY);

        //设置接口引用是否设置了多个组
        String group = directoryUrl.getParameter(Constants.GROUP_KEY, "");
        this.multiGroup = "*".equals(group) || group.contains(",");

        //设置接口引用所要应用的方法的名字列表
        String methods = queryMap.get(Constants.METHODS_KEY);
        this.serviceMethods = methods == null ? null : Constants.COMMA_SPLIT_PATTERN.split(methods);
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    /**
     * 订阅即是设置消费url
     * 也就是目录服务需要消费的url
     * 同时注册中心注册相应的url
     *
     * @param url 元信息
     */
    public void subscribe(URL url) {
        setConsumerUrl(url);
        registry.subscribe(url, this);
    }

    public void destroy() {
        if (isDestroyed()) {
            return;
        }
        // unsubscribe.
        try {
            if (getConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getConsumerUrl(), this);
            }
        } catch (Throwable t) {
            logger.warn("unexpeced error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy(); // 必须在unsubscribe之后执行
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    /**
     * 当相关的urls发生变化时，进行通知。
     * <p>
     * 处理相关的url列表,对其目录属性进行处理。对于不支持的目录属性，将会记录日志进行警告
     * <ul>
     * <li>构建invoker相关的urls:目录为providers</li><br/>
     * <li>构建router相关的urls:目录为routers或者协议为router</li><br/>
     * <li>构建configurator相关的的urls:目录为configurators或者协议为override</li><br/>
     * </ul>
     * </p>
     *
     * @param urls 相关的已注册信息列表，总不为空，含义同{@link com.alibaba.dubbo.registry.RegistryService#lookup(URL)}的返回值。
     */
    public synchronized void notify(List<URL> urls) {
        List<URL> invokerUrls = new ArrayList<URL>();
        List<URL> routerUrls = new ArrayList<URL>();
        List<URL> configuratorUrls = new ArrayList<URL>();
        // 遍历分类，分类管理
        // 目录分类是router || 协议是routers加入路由列表（routerUrls）中
        // 目录分类是configurators || 协议是override加入配置列表（configuratorUrls）中
        // 目录分类是providers加入调用者列表（invokerUrls）中
        for (URL url : urls) {
            // 获得url中的协议属性
            String protocol = url.getProtocol();
            // 获得url中的目录属性（默认为providers)
            String category = url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
            if (Constants.ROUTERS_CATEGORY.equals(category) || Constants.ROUTE_PROTOCOL.equals(protocol)) {
                routerUrls.add(url);
            } else if (Constants.CONFIGURATORS_CATEGORY.equals(category) || Constants.OVERRIDE_PROTOCOL.equals(protocol)) {
                configuratorUrls.add(url);
            } else if (Constants.PROVIDERS_CATEGORY.equals(category)) {
                invokerUrls.add(url);
            } else {
                logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
            }
        }

        // configurators 的处理
        if (configuratorUrls != null && configuratorUrls.size() > 0) {
            this.configurators = toConfigurators(configuratorUrls);
        }

        // routers 的处理
        if (routerUrls != null && routerUrls.size() > 0) {
            List<Router> routers = toRouters(routerUrls);
            if (routers != null) { // null - do nothing
                setRouters(routers);
            }
        }
        List<Configurator> localConfigurators = this.configurators; // local reference

        // 合并override参数
        this.overrideDirectoryUrl = directoryUrl;
        if (localConfigurators != null && localConfigurators.size() > 0) {
            for (Configurator configurator : localConfigurators) {
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }
        // providers
        refreshInvoker(invokerUrls);
    }


    /**
     * 根据invokerURL列表转换为invoker列表。转换规则如下：
     * 1.如果url已经被转换为invoker，则不在重新引用，直接从缓存中获取，注意如果url中任何一个参数变更也会重新引用
     * 2.如果传入的invoker列表不为空，则表示最新的invoker列表
     * 3.如果传入的invokerUrl列表是空，则表示只是下发的override规则或route规则，需要重新交叉对比，决定是否需要重新引用。
     *
     * @param invokerUrls 传入的参数不能为null
     */
    private void refreshInvoker(List<URL> invokerUrls) {
        if (invokerUrls != null && invokerUrls.size() == 1 && invokerUrls.get(0) != null
                && Constants.EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
            //只有一个，且他的协议是empty，也就是消费方接口，目录服务消费的url在zk上构建的zknode目录为provider下没有节点数据
            //也就是在注册中心上找不到该对应的服务提供方
            this.forbidden = true; // 禁止访问
            this.methodInvokerMap = null; // 置空列表
            destroyAllInvokers(); // 关闭所有Invoker
        } else {
            this.forbidden = false; // 允许访问
            Map<String, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap; // local reference
            if (invokerUrls.size() == 0 && this.cachedInvokerUrls != null) {
                //使用缓存
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
                //加入缓存中
                this.cachedInvokerUrls = new HashSet<URL>();
                this.cachedInvokerUrls.addAll(invokerUrls);//缓存invokerUrls列表，便于交叉对比
            }
            if (invokerUrls.size() == 0) {
                return;
            }
            //将url映射成invoker映射
            Map<String, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);// 将URL列表转成Invoker列表
            //将invoker映射转换成方法级别的invoker映射
            Map<String, List<Invoker<T>>> newMethodInvokerMap = toMethodInvokers(newUrlInvokerMap); // 换方法名映射Invoker列表

            // state change
            //如果计算错误，则不进行处理.
            if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls.toString()));
                return;
            }
            //消费方引用接口如果属于多个组，则继续处理，否则直接使用方法级别的invoker映射
            this.methodInvokerMap = multiGroup ? toMergeMethodInvokerMap(newMethodInvokerMap) : newMethodInvokerMap;
            //设定url级别的invoker的映射
            this.urlInvokerMap = newUrlInvokerMap;
            try {
                destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap); // 关闭未使用的Invoker
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }
        }
    }

    /**
     * 合并相关invoker
     *
     * @param methodMap
     * @return
     */
    private Map<String, List<Invoker<T>>> toMergeMethodInvokerMap(Map<String, List<Invoker<T>>> methodMap) {
        Map<String, List<Invoker<T>>> result = new HashMap<String, List<Invoker<T>>>();
        for (Map.Entry<String, List<Invoker<T>>> entry : methodMap.entrySet()) {
            String method = entry.getKey();
            List<Invoker<T>> invokers = entry.getValue();
            Map<String, List<Invoker<T>>> groupMap = new HashMap<String, List<Invoker<T>>>();
            for (Invoker<T> invoker : invokers) {
                String group = invoker.getUrl().getParameter(Constants.GROUP_KEY, "");
                List<Invoker<T>> groupInvokers = groupMap.get(group);
                if (groupInvokers == null) {
                    groupInvokers = new ArrayList<Invoker<T>>();
                    groupMap.put(group, groupInvokers);
                }
                groupInvokers.add(invoker);
            }
            if (groupMap.size() == 1) {
                result.put(method, groupMap.values().iterator().next());
            } else if (groupMap.size() > 1) {
                List<Invoker<T>> groupInvokers = new ArrayList<Invoker<T>>();
                for (List<Invoker<T>> groupList : groupMap.values()) {
                    groupInvokers.add(cluster.join(new StaticDirectory<T>(groupList)));
                }
                result.put(method, groupInvokers);
            } else {
                result.put(method, invokers);
            }
        }
        return result;
    }

    /**
     * 将overrideURL转换为map，供重新refer时使用.
     * 每次下发全部规则，全部重新组装计算
     * <p>
     * 入参urls的契约：
     * 1.override://0.0.0.0/...(或override://ip:port...?anyhost=true)&para1=value1...表示全局规则(对所有的提供者全部生效)<br/>
     * 2.override://ip:port...?anyhost=false 特例规则（只针对某个提供者生效<br>
     * 3.不支持override://规则... 需要注册中心自行计算.<br/>
     * 4.不带参数的override://0.0.0.0/ 表示清除override<br/>
     *
     * @param urls 元信息
     * @return 配置对象
     */
    public static List<Configurator> toConfigurators(List<URL> urls) {
        List<Configurator> configurators = new ArrayList<Configurator>(urls.size());
        if (urls == null || urls.size() == 0) {
            return configurators;
        }
        for (URL url : urls) {
            //url中含有empty，则清空所有，返回一个空的配置项
            if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                configurators.clear();
                break;
            }
            //获得url中的参数
            Map<String, String> override = new HashMap<String, String>(url.getParameters());
            //override 上的anyhost可能是自动添加的，不能影响改变url判断
            override.remove(Constants.ANYHOST_KEY);
            if (override.size() == 0) {
                //对于遇见空参数，清空全部，并继续？？why is not break;
                //上面的原因，继续是合理的，因为有其他选项
                configurators.clear();
                continue;
            }
            configurators.add(configuratorFactory.getConfigurator(url));
        }
        //对其进行一个配置
        Collections.sort(configurators);
        return configurators;
    }

    /**
     * @param urls
     * @return null : no routers ,do nothing
     * else :routers list
     */
    private List<Router> toRouters(List<URL> urls) {
        List<Router> routers = new ArrayList<Router>();
        if (urls == null || urls.size() == 0) {
            return routers;
        }
        for (URL url : urls) {
            if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                continue;
            }
            String routerType = url.getParameter(Constants.ROUTER_KEY);
            if (routerType != null && routerType.length() > 0) {
                url = url.setProtocol(routerType);
            }
            try {
                Router router = routerFactory.getRouter(url);
                if (!routers.contains(router))
                    routers.add(router);
            } catch (Throwable t) {
                logger.error("convert router url to router error, url: " + url, t);
            }
        }
        return routers;
    }

    /**
     * 对注册在provider不同的服务，实现对转换为消费方的invoker，
     * 服务提供方方可能是集群，那么多个invoker是对的，
     * 多个服务方对节点同时写入，也会形成多个invoker。
     * 将urls转成invokers,如果url已经被refer过，不再重新引用。
     *
     * @param urls 元信息列表(服务方元信息)，一个或者多个，多个的情况是多个服务方注册同一个服务
     * @return invokers 对应的invokers
     */
    private Map<String, Invoker<T>> toInvokers(List<URL> urls) {
        //新的缓存结构
        Map<String, Invoker<T>> newUrlInvokerMap = new HashMap<String, Invoker<T>>();
        if (urls == null || urls.size() == 0) {
            return newUrlInvokerMap;
        }
        Set<String> keys = new HashSet<String>();
        //获取消费方配置的协议,
        String queryProtocols = this.queryMap.get(Constants.PROTOCOL_KEY);
        for (URL providerUrl : urls) {

            //empty的产生是由于目录下木有相关的节点而生成的一个占位，因此这里要忽略掉,实际上一般不会发生。忽略配置为empty的元信息
            if (Constants.EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                continue;
            }

            //验证下是否支持配置,不支持记录错误日记，忽略掉
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() + " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost()
                        + ", supported protocol: " + ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
            }

            //如果reference端配置了protocol（可以是多个），则只选择匹配的protocol(多个服务方会有多个urls，选择合适且正确)
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                String[] acceptProtocols = queryProtocols.split(",");
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                if (!accept) {
                    continue;
                }
            }

            //合并参数
            URL url = mergeUrl(providerUrl);
            String key = url.toFullString(); // URL参数是排序的
            if (keys.contains(key)) { // 重复URL
                continue;
            }
            keys.add(key);

            // 缓存key为没有合并消费端参数的URL，不管消费端如何合并参数，如果服务端URL发生变化，则重新refer
            // 尝试先从缓存中获取，缓存中已经有的话，不需要重新去引用远程的服务
            Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(key);
            if (invoker == null) {
                // 缓存中没有，重新refer
                try {
                    boolean enabled = true;
                    if (url.hasParameter(Constants.DISABLED_KEY)) {
                        //是否含有disable键
                        enabled = !url.getParameter(Constants.DISABLED_KEY, false);
                    } else {
                        //使用enable键来设置
                        enabled = url.getParameter(Constants.ENABLED_KEY, true);
                    }
                    if (enabled) {
                        //url是可以被转换为invoker的
                        //根据远程注册中心上的相关目录服务下的信息的url，来实现对服务提供方的引用
                        //这个引用总是使用InvokerDelegete进行包装
                        //其中url是合并了远程提供方的相关信息的url
                        invoker = new InvokerDelegate<T>(protocol.refer(serviceType, url), url, providerUrl);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) {
                    // 将新的引用放入缓存
                    newUrlInvokerMap.put(key, invoker);
                }
            } else {
                newUrlInvokerMap.put(key, invoker);
            }
        }
        keys.clear();
        return newUrlInvokerMap;
    }

    /**
     * 合并url参数 顺序为override > -D >Consumer > Provider
     *
     * @param providerUrl
     * @return
     */
    private URL mergeUrl(URL providerUrl) {

        //本地消费端配置的参数，和远程参数进行合并
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap);

        //使用configurators再次进行合并
        List<Configurator> localConfigurators = this.configurators; // local reference
        if (localConfigurators != null && localConfigurators.size() > 0) {
            for (Configurator configurator : localConfigurators) {
                providerUrl = configurator.configure(providerUrl);
            }
        }

        providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false)); // 不检查连接是否成功，总是创建Invoker！

        //directoryUrl 与 override 合并是在notify的最后，这里不能够处理
        this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerUrl.getParameters()); // 合并提供者参数        

        if ((providerUrl.getPath() == null || providerUrl.getPath().length() == 0) && "dubbo".equals(providerUrl.getProtocol())) { // 兼容1.0
            //fix by tony.chenl DUBBO-44
            String path = directoryUrl.getParameter(Constants.INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
    }

    /**
     * 路由
     * @param invokers
     * @param method
     * @return
     */
    private List<Invoker<T>> route(List<Invoker<T>> invokers, String method) {
        Invocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
        List<Router> routers = getRouters();
        if (routers != null) {
            for (Router router : routers) {
                if (router.getUrl() != null && !router.getUrl().getParameter(Constants.RUNTIME_KEY, true)) {
                    invokers = router.route(invokers, getConsumerUrl(), invocation);
                }
            }
        }
        return invokers;
    }

    /**
     * 将invokers列表转成与方法的映射关系
     *
     * @param invokersMap url级别上的invoker映射
     * @return Invoker与方法的映射关系
     */
    private Map<String, List<Invoker<T>>> toMethodInvokers(Map<String, Invoker<T>> invokersMap) {
        Map<String, List<Invoker<T>>> newMethodInvokerMap = new HashMap<String, List<Invoker<T>>>();
        // 按提供者URL所声明的methods分类，兼容注册中心执行路由过滤掉的methods
        List<Invoker<T>> invokersList = new ArrayList<Invoker<T>>();
        if (invokersMap != null && invokersMap.size() > 0) {
            for (Invoker<T> invoker : invokersMap.values()) {
                //获得invoker上的方法参数
                String parameter = invoker.getUrl().getParameter(Constants.METHODS_KEY);
                if (parameter != null && parameter.length() > 0) {
                    //拆分方法参数，确定到每一个方法
                    String[] methods = Constants.COMMA_SPLIT_PATTERN.split(parameter);
                    for (String method : methods) {
                        if (method != null && method.length() > 0 && !Constants.ANY_VALUE.equals(method)) {
                            //对于非通配符而是具体的方法的处理，将将方法和invoker构建相应的映射。
                            //当然方法名对应的是一个invoker列表这一点，有服务提供方的集群决定的。
                            List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                            if (methodInvokers == null) {
                                methodInvokers = new ArrayList<Invoker<T>>();
                                newMethodInvokerMap.put(method, methodInvokers);
                            }
                            methodInvokers.add(invoker);
                        }
                    }
                }
                invokersList.add(invoker);
            }
        }
        //设置通配的invoker，通配自然对应了所有的invoker列表。
        newMethodInvokerMap.put(Constants.ANY_VALUE, invokersList);
        if (serviceMethods != null && serviceMethods.length > 0) {
            //进行处理方法级别
            for (String method : serviceMethods) {
                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                if (methodInvokers == null || methodInvokers.size() == 0) {
                    methodInvokers = invokersList;
                }
                newMethodInvokerMap.put(method, route(methodInvokers, method));
            }
        }
        // sort and unmodifiable
        for (String method : new HashSet<String>(newMethodInvokerMap.keySet())) {
            List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
            Collections.sort(methodInvokers, InvokerComparator.getComparator());
            newMethodInvokerMap.put(method, Collections.unmodifiableList(methodInvokers));
        }
        return Collections.unmodifiableMap(newMethodInvokerMap);
    }

    /**
     * 关闭所有Invoker
     */
    private void destroyAllInvokers() {
        Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (localUrlInvokerMap != null) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                try {
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }
        methodInvokerMap = null;
    }

    /**
     * 检查缓存中的invoker是否需要被destroy
     * 如果url中指定refer.autodestroy=false，则只增加不减少，可能会有refer泄漏，
     *
     * @param oldUrlInvokerMap 老的url级别的invoker映射关系
     * @param newUrlInvokerMap 新的url级别的invoker映射关系
     */
    private void destroyUnusedInvokers(Map<String, Invoker<T>> oldUrlInvokerMap, Map<String, Invoker<T>> newUrlInvokerMap) {
        //新的url级别的映射为空，说明现在没有任何服务提供方法，直接销毁所有的invoker
        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            destroyAllInvokers();
            return;
        }

        //构造需要删除的列表
        List<String> deleted = new ArrayList<String>();
        if (oldUrlInvokerMap != null) {
            Collection<Invoker<T>> newInvokers = newUrlInvokerMap.values();
            for (Map.Entry<String, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {
                if (!newInvokers.contains(entry.getValue())) {
                    deleted.add(entry.getKey());
                }
            }
        }

        //进行相关的销毁
        for (String url : deleted) {
            Invoker<T> invoker = oldUrlInvokerMap.remove(url);
            if (invoker != null) {
                try {
                    invoker.destroy();
                    if (logger.isDebugEnabled()) {
                        logger.debug("destory invoker[" + invoker.getUrl() + "] success. ");
                    }
                } catch (Exception e) {
                    logger.warn("destory invoker[" + invoker.getUrl() + "] faild. " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 目录服务根据调用对象寻找与之对应的调用者列表
     *
     * @param invocation 调用对象
     * @return 对应的调用者
     */
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden) {
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "Forbid consumer " + NetUtils.getLocalHost() + " access service " + getInterface().getName() + " from registry " + getUrl().getAddress() + " use dubbo version " + Version.getVersion() + ", Please check registry access list (whitelist/blacklist).");
        }

        List<Invoker<T>> invokers = null;
        //使用缓存中进行调用，一个方法可能对应一个invoker列表，局部引用，防止缓存突然变化
        Map<String, List<Invoker<T>>> localMethodInvokerMap = this.methodInvokerMap; // local reference
        if (localMethodInvokerMap != null && localMethodInvokerMap.size() > 0) {
            //从调用对象中获取需要调用分方法名
            String methodName = RpcUtils.getMethodName(invocation);
            //获得方法对应的参数
            Object[] args = RpcUtils.getArguments(invocation);
            if (args != null && args.length > 0 && args[0] != null
                    && (args[0] instanceof String || args[0].getClass().isEnum())) {
                //在第一个参数是String或是是Enum的类型，直接从缓存中获取
                invokers = localMethodInvokerMap.get(methodName + "." + args[0]); // 可根据第一个参数枚举路由
            }
            if (invokers == null) {
                //尝试使用另一个key来获取
                invokers = localMethodInvokerMap.get(methodName);
            }
            if (invokers == null) {
                //尝试使用另一个key来获取
                invokers = localMethodInvokerMap.get(Constants.ANY_VALUE);
            }
            if (invokers == null) {
                //尝试使用默认策略获取，这tm的是什么策略
                Iterator<List<Invoker<T>>> iterator = localMethodInvokerMap.values().iterator();
                if (iterator.hasNext()) {
                    invokers = iterator.next();
                }
            }
        }
        return invokers == null ? new ArrayList<Invoker<T>>(0) : invokers;
    }

    public Class<T> getInterface() {
        return serviceType;
    }

    /**
     * tip 特别的注意点，
     * 注册中心的目录服务覆盖了其父类的方法，
     * 返回的是一个overrideDirectoryUrl
     *
     * @return overrideDirectoryUrl
     * @see #RegistryDirectory(Class, URL)
     * @see AbstractDirectory#getUrl()
     */
    public URL getUrl() {
        return this.overrideDirectoryUrl;
    }

    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<String, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<String, List<Invoker<T>>> getMethodInvokerMap() {
        return methodInvokerMap;
    }

    private static class InvokerComparator implements Comparator<Invoker<?>> {

        private static final InvokerComparator comparator = new InvokerComparator();

        public static InvokerComparator getComparator() {
            return comparator;
        }

        private InvokerComparator() {
        }

        public int compare(Invoker<?> o1, Invoker<?> o2) {
            return o1.getUrl().toString().compareTo(o2.getUrl().toString());
        }

    }

    /**
     * 代理类，主要用于存储注册中心下发的url地址，用于重新重新refer时能够根据providerURL queryMap overrideMap重新组装
     * url是合并过参数的url
     * providerurl是合并之前的url也就是服务提供方的url信息
     *
     * @param <T>
     * @author chao.liuc
     */
    private static class InvokerDelegate<T> extends InvokerWrapper<T> {

        //服务提供方的相关参数
        private URL providerUrl;

        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }
}