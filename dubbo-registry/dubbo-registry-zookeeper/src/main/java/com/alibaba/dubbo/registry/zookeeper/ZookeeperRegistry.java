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
package com.alibaba.dubbo.registry.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.dubbo.remoting.zookeeper.ChildListener;
import com.alibaba.dubbo.remoting.zookeeper.ZookeeperClient;
import com.alibaba.dubbo.remoting.zookeeper.StateListener;
import com.alibaba.dubbo.remoting.zookeeper.ZookeeperTransporter;
import com.alibaba.dubbo.rpc.RpcException;

/**
 * ZookeeperRegistry(zk注册中心)
 *
 * @author william.liangf
 */
public class ZookeeperRegistry extends FailbackRegistry {

    private final static Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);

    //zk默认端口
    private final static int DEFAULT_ZOOKEEPER_PORT = 2181;

    //默认的应用路径/dubbo
    private final static String DEFAULT_ROOT = "dubbo";

    //zk上应用路径
    private final String root;

    private final Set<String> anyServices = new ConcurrentHashSet<String>();

    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners = new ConcurrentHashMap<URL, ConcurrentMap<NotifyListener, ChildListener>>();

    //zk客户端封装实例
    private final ZookeeperClient zkClient;

    /**
     * 生成zookeeper的注册中心实例
     * <ul>
     * <li>校验url中地址的合理性</li><br/>
     * <li>获得group:来自url中key为{@link Constants#GROUP_KEY}的值，默认是{@link #DEFAULT_ROOT}</li><br/>
     * <li>使用group，设定root属性，即zk应用路径</li><br/>
     * <li>从zk客户端转换器中获得zk客户端(封装ZkClient or Curator)</li><br/>
     * <li>为zk客户端添加状态监听器，失败的时候进行复原</li><br/>
     * </ul>
     *
     * @param url                  注册的URL
     * @param zookeeperTransporter zk客户端转换器
     * @see FailbackRegistry#FailbackRegistry(URL)
     */
    public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        //父类构造函数
        super(url);
        //校验合理性
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        //获得组信息,默认是dubbo(一般情况下都是默认的，很少有开发者会配置注册中心的group)
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        //尝试为group添加前缀
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        //接口对应的顶层节点路径
        this.root = group;
        //连接到zk
        zkClient = zookeeperTransporter.connect(url);
        //添加监听器
        zkClient.addStateListener(new StateListener() {
            public void stateChanged(int state) {
                if (state == RECONNECTED) {
                    try {
                        recover();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
    }

    /**
     * 检测zk可用性
     *
     * @return
     */
    @Override
    public boolean isAvailable() {
        return zkClient.isConnected();
    }

    /**
     *
     */
    @Override
    public void destroy() {
        super.destroy();
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * zk注册中心处理注册URL的逻辑<br/>
     * <ul>
     * <li>使用zk客户端在zk上创建相应的路径</li><br/>
     * <li>存储节点的形式，有url中键{@link Constants#DYNAMIC_KEY}觉得默认是false，临时节点</li><br/>
     * </ul>
     *
     * @param url 注册的url
     * @apiNote 当URL设置了dynamic=false参数，则需持久存储，否则，当注册者出现断电等情况异常退出时，需自动删除。
     * @see FailbackRegistry#register(URL)
     * @see #toUrlPath(URL)
     * @see com.alibaba.dubbo.registry.Registry#register(URL)
     */
    @Override
    protected void doRegister(URL url) {
        try {
            zkClient.create(toUrlPath(url), url.getParameter(Constants.DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doUnregister(URL url) {
        try {
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 构建监听对url的订阅
     * @param url 受订阅的目标url
     * @param listener url的监听者
     * @see FailbackRegistry#subscribe(URL, NotifyListener)
     */
    @Override
    protected void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            //泛化调用的情况
            if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
                //获得顶层路径，由于是泛化调用没有其他的下面的路径了
                String root = toRootPath();
                //获得url的监听者，无则新建
                ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
                if (listeners == null) {
                    zkListeners.putIfAbsent(url, new ConcurrentHashMap<NotifyListener, ChildListener>());
                    listeners = zkListeners.get(url);
                }
                //获得监听者的实际监听者
                ChildListener zkListener = listeners.get(listener);
                //子监听者为空的情况下，创建实际的监听者
                if (zkListener == null) {
                    listeners.putIfAbsent(listener, new ChildListener() {
                        //上一级发生变化时，进行回调，实现对子路径的监听
                        //由于是*，所以parentPath是更高一层的节点，也就是currentChilds，是各种接口的名字
                        public void childChanged(String parentPath, List<String> currentChilds) {
                            for (String child : currentChilds) {
                                child = URL.decode(child);
                                //缓存中不包含相关相关的订阅，需要进行重新订阅
                                if (!anyServices.contains(child)) {
                                    anyServices.add(child);
                                    //对新增加的代表引用接口的节点进行新的订阅
                                    subscribe(url.setPath(child).addParameters(Constants.INTERFACE_KEY, child,
                                            Constants.CHECK_KEY, String.valueOf(false)), listener);
                                }
                            }
                        }
                    });
                    zkListener = listeners.get(listener);
                }
                zkClient.create(root, false);
                //为顶层节点添加监听器
                List<String> services = zkClient.addChildListener(root, zkListener);
                //第一次，添加所有的节点服务，因此不需要判断，全部加进来
                if (services != null && services.size() > 0) {
                    for (String service : services) {
                        service = URL.decode(service);
                        anyServices.add(service);
                        subscribe(url.setPath(service).addParameters(Constants.INTERFACE_KEY, service,
                                Constants.CHECK_KEY, String.valueOf(false)), listener);
                    }
                }
            } else {
                //不是泛化接口的调用
                List<URL> urls = new ArrayList<URL>();
                //处理url中的目录信息
                for (String path : toCategoriesPath(url)) {
                    ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
                    if (listeners == null) {
                        zkListeners.putIfAbsent(url, new ConcurrentHashMap<NotifyListener, ChildListener>());
                        listeners = zkListeners.get(url);
                    }
                    ChildListener zkListener = listeners.get(listener);
                    if (zkListener == null) {
                        listeners.putIfAbsent(listener, new ChildListener() {
                            public void childChanged(String parentPath, List<String> currentChilds) {
                                ZookeeperRegistry.this.notify(url, listener, toUrlsWithEmpty(url, parentPath, currentChilds));
                            }
                        });
                        zkListener = listeners.get(listener);
                    }
                    //创建到目录的路劲，默认为/dubbo/接口名/path，永久节点
                    zkClient.create(path, false);
                    //为path添加监听，一旦path下的节点发生变化，调用监听的方法
                    List<String> children = zkClient.addChildListener(path, zkListener);
                    if (children != null) {
                        urls.addAll(toUrlsWithEmpty(url, path, children));
                    }
                }
                //进行通知
                notify(url, listener, urls);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doUnsubscribe(URL url, NotifyListener listener) {
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
        if (listeners != null) {
            ChildListener zkListener = listeners.get(listener);
            if (zkListener != null) {
                zkClient.removeChildListener(toUrlPath(url), zkListener);
            }
        }
    }

    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            List<String> providers = new ArrayList<String>();
            for (String path : toCategoriesPath(url)) {
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 生成应用在zk上的生成的路径（目录）
     *
     * @return 应用路径的目录形式
     */
    private String toRootDir() {
        if (root.equals(Constants.PATH_SEPARATOR)) {
            return root;
        }
        return root + Constants.PATH_SEPARATOR;
    }

    /**
     * 返回zk上的应用路径，默认是/dubbo
     *
     * @return 应用路径
     */
    private String toRootPath() {
        return root;
    }

    /**
     * 生成暴露服务类在zk上对应的注册路径
     * 对于暴露服务类的名字为{@link Constants#ANY_VALUE}。直接返回项目应用路径{@link #root}，默认是/dubbo
     * 对于暴露服务类的有确定的名字，项目应用路径{@link #root}+/+name
     *
     * @param url 注册url
     * @return 服务的路径
     * @see URL#getServiceInterface()
     * @see #toRootDir()
     * @see #toRootPath()
     */
    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        if (Constants.ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        return toRootDir() + URL.encode(name);
    }

    /**
     * 根据url中的键{@link Constants#CATEGORY_KEY}，生成相关zk上子目录的名字
     * <ul>
     * <li>对于对应的值为{@link Constants#ANY_VALUE},使用[providers，consumers，routers，configurators]</li><br/>
     * <li>否则尝试通过其值解析，ex：v equal to aa,bb and the result is [aa,bb]. 没有值的情况下使用[providers]</li><br/>
     * </ul>
     *
     * @param url 注册的url
     * @return 为暴露的服务添加子目录，返回到子目录的完整路径。多个
     * @see #toServicePath(URL)
     */
    private String[] toCategoriesPath(URL url) {
        String[] categroies;
        if (Constants.ANY_VALUE.equals(url.getParameter(Constants.CATEGORY_KEY))) {
            categroies = new String[]{Constants.PROVIDERS_CATEGORY, Constants.CONSUMERS_CATEGORY,
                    Constants.ROUTERS_CATEGORY, Constants.CONFIGURATORS_CATEGORY};
        } else {
            categroies = url.getParameter(Constants.CATEGORY_KEY, new String[]{Constants.DEFAULT_CATEGORY});
        }
        String[] paths = new String[categroies.length];
        for (int i = 0; i < categroies.length; i++) {
            paths[i] = toServicePath(url) + Constants.PATH_SEPARATOR + categroies[i];
        }
        return paths;
    }

    /**
     * 根据url中的键{@link Constants#CATEGORY_KEY}，生成相关zk上子目录的名字
     * 默认使用providers做为值
     *
     * @param url 注册的url
     * @return 为暴露的服务添加子目录，返回到子目录的完整路径。单个
     */
    private String toCategoryPath(URL url) {
        return toServicePath(url) + Constants.PATH_SEPARATOR + url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
    }

    /**
     * 生成完整的在zk上注册的暴露服务的路径
     *
     * @param url 注册的url
     * @return 为暴露的服务添加子目录，返回到子目录的完整路径：应用目录/暴露服务全类名/子目录/url的string值
     */
    private String toUrlPath(URL url) {
        return toCategoryPath(url) + Constants.PATH_SEPARATOR + URL.encode(url.toFullString());
    }


    /**
     *
     * @param consumer 主url，消费方url
     * @param providers 子节点
     * @return 子节点能和主url匹配上的，返回
     */
    private List<URL> toUrlsWithoutEmpty(URL consumer, List<String> providers) {
        List<URL> urls = new ArrayList<URL>();
        if (providers != null && providers.size() > 0) {
            for (String provider : providers) {
                provider = URL.decode(provider);
                if (provider.contains("://")) {
                    URL url = URL.valueOf(provider);
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }


    /**
     *
     * @param consumer 主url
     * @param path 主url搞出来的某个目录对应的路径
     * @param providers 目录下子节点名称
     * @return
     */
    private List<URL> toUrlsWithEmpty(URL consumer, String path, List<String> providers) {
        List<URL> urls = toUrlsWithoutEmpty(consumer, providers);
        //目录下没有能匹配consumer的url的处理
        if (urls == null || urls.isEmpty()) {
            int i = path.lastIndexOf('/');
            //获得目录名字
            String category = i < 0 ? path : path.substring(i + 1);
            //为consumer打个桩，将其协议设定为empty，目录服务设定为当前的目录
            URL empty = consumer.setProtocol(Constants.EMPTY_PROTOCOL).addParameter(Constants.CATEGORY_KEY, category);
            urls.add(empty);
        }
        return urls;
    }

    /**
     * 为zk地址添加默认的端口号
     * ex
     * address like 127.0.0.1 and the result is 127.0.0.1:2181
     *
     * @param address zk地址
     * @return
     */
    static String appendDefaultPort(String address) {
        if (address != null && address.length() > 0) {
            int i = address.indexOf(':');
            if (i < 0) {
                return address + ":" + DEFAULT_ZOOKEEPER_PORT;
            } else if (Integer.parseInt(address.substring(i + 1)) == 0) {
                return address.substring(0, i + 1) + DEFAULT_ZOOKEEPER_PORT;
            }
        }
        return address;
    }

}