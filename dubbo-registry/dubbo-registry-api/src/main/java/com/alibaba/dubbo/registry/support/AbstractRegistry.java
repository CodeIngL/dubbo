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
package com.alibaba.dubbo.registry.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;

/**
 * AbstractRegistry. (SPI, Prototype, ThreadSafe)
 *
 * @author chao.liuc
 * @author william.liangf
 */
public abstract class AbstractRegistry implements Registry {

    // 日志输出
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // URL地址分隔符，用于文件缓存中，服务提供者URL分隔
    private static final char URL_SEPARATOR = ' ';

    // URL地址分隔正则表达式，用于解析文件缓存中服务提供者URL列表
    private static final String URL_SPLIT = "\\s+";

    // 注册的url
    private URL registryUrl;

    // 本地磁盘缓存文件
    private File file;

    // 本地磁盘缓存，其中特殊的key值.registies记录注册中心列表，其它均为notified服务提供者列表
    private final Properties properties = new Properties();

    // 文件缓存定时写入
    private final ExecutorService registryCacheExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveRegistryCache", true));

    //是否是同步保存文件
    private final boolean syncSaveFile;

    private final AtomicLong lastCacheChanged = new AtomicLong();

    private final Set<URL> registered = new ConcurrentHashSet<URL>();

    /**
     * <p>
     * 受订阅的url和对其监听的监听者集合<br/>
     * key:url,value:对url监听的监听者集合
     * </p>
     */
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<URL, Set<NotifyListener>>();

    //key：URL  v:Map--->key:url的分组依据，v：同一组URL集合
    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<URL, Map<String, List<URL>>>();

    /**
     * 抽象类AbstractRegistry构造函数，服务于抽象类FailbackRegistry，dubbo对注册中心概念的抽象
     * <ul>
     * <li>保存注册的url:{@link #registryUrl}</li><br/>
     * <li>设定保存文件的标志{@link #syncSaveFile}。从url中键为{@link Constants#REGISTRY_FILESAVE_SYNC_KEY}对应的值，默认是false（异步保存）</li><br/>
     * <li>设定保存文件路劲{@link #file}。从url中键为{@link Constants#FILE_KEY}对应的值，默认是System.getProperty("user.home") + "/.dubbo/dubbo-registry-" + url.getHost() + ".cache"</li><br/>
     * <li>加载文件中的配置到{@link #properties}</li><br/>
     * <li>通知整个集群地址</li><br/>
     * </ul>
     *
     * @param url 注册的url
     * @see #loadProperties()
     * @see #notify(List)
     */
    public AbstractRegistry(URL url) {
        // 设置url
        setUrl(url);
        // 启动文件保存定时器:key:save.file 默认false（异步保存）
        syncSaveFile = url.getParameter(Constants.REGISTRY_FILESAVE_SYNC_KEY, false);
        // 获得url中file的的值，默认$user.home$/.dubbo/dubbo-registry-url的host.cache
        String filename = url.getParameter(Constants.FILE_KEY, System.getProperty("user.home") + "/.dubbo/dubbo-registry-" + url.getHost() + ".cache");

        // 检查文件存在否，不存在相应处理
        File file = null;
        if (ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException("Invalid registry store file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
        }
        // 本地磁盘文件
        this.file = file;

        // 加载文件配置
        loadProperties();

        // 新创建的注册中心，注册了也要通知其他方，以免遗漏
        // 通知整个集群地址
        notify(url.getBackupUrls());
    }

    /**
     * 设置注册中心url
     *
     * @param url 注册中心url
     */
    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    public URL getUrl() {
        return registryUrl;
    }

    public Set<URL> getRegistered() {
        return registered;
    }

    public Map<URL, Set<NotifyListener>> getSubscribed() {
        return subscribed;
    }

    public Map<URL, Map<String, List<URL>>> getNotified() {
        return notified;
    }


    public File getCacheFile() {
        return file;
    }

    public Properties getCacheProperties() {
        return properties;
    }

    public AtomicLong getLastCacheChanged() {
        return lastCacheChanged;
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        public void run() {
            doSaveProperties(version);
        }
    }

    /**
     * 保存信息到文件中
     * <ul>
     * <li>先读取文件，在内存中形成键值对newProperties</li><br/>
     * <li>将缓存{@link #properties}合并到newProperties中</li><br/>
     * <li>写入新建的文件</li><br/>
     * </ul>
     *
     * @param version
     */
    public void doSaveProperties(long version) {
        //校验下新旧的关系
        if (version < lastCacheChanged.get()) {
            return;
        }
        //文件不存在直接返回
        if (file == null) {
            return;
        }
        Properties newProperties = new Properties();
        // 保存之前先读取一遍，防止多个注册中心之间冲突
        InputStream in = null;
        try {
            if (file.exists()) {
                in = new FileInputStream(file);
                newProperties.load(in);
            }
        } catch (Throwable e) {
            logger.warn("Failed to load registry store file, cause: " + e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
        // 保存
        try {
            //使用内存的配置，覆盖掉原来的所有的配置
            newProperties.putAll(properties);
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            //防止竞争的设计
            RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
            try {
                FileChannel channel = raf.getChannel();
                try {
                    FileLock lock = channel.tryLock();
                    if (lock == null) {
                        throw new IOException("Can not lock the registry cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");
                    }
                    // 保存
                    try {
                        if (!file.exists()) {
                            file.createNewFile();
                        }
                        FileOutputStream outputFile = new FileOutputStream(file);
                        try {
                            newProperties.store(outputFile, "Dubbo Registry Cache");
                        } finally {
                            outputFile.close();
                        }
                    } finally {
                        lock.release();
                    }
                } finally {
                    channel.close();
                }
            } finally {
                raf.close();
            }
        } catch (Throwable e) {
            //失败后异步执行
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                registryCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save registry store file, cause: " + e.getMessage(), e);
        }
    }

    /**
     * 尝试加载文件中的配置（键值对）到{@link #properties},如果文件存在的话
     */
    private void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load registry store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load registry store file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * @param url
     * @return
     */
    public List<URL> getCacheUrls(URL url) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key != null && key.length() > 0
                    && key.equals(url.getServiceKey())
                    && (Character.isLetter(key.charAt(0)) || key.charAt(0) == '_')
                    && value != null && value.length() > 0) {
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<URL>();
                for (String u : arr) {
                    urls.add(URL.valueOf(u));
                }
                return urls;
            }
        }
        return null;
    }

    public List<URL> lookup(URL url) {
        List<URL> result = new ArrayList<URL>();
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        if (notifiedUrls != null && notifiedUrls.size() > 0) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    if (!Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        } else {
            final AtomicReference<List<URL>> reference = new AtomicReference<List<URL>>();
            NotifyListener listener = new NotifyListener() {
                public void notify(List<URL> urls) {
                    reference.set(urls);
                }
            };
            subscribe(url, listener); // 订阅逻辑保证第一次notify后再返回
            List<URL> urls = reference.get();
            if (urls != null && urls.size() > 0) {
                for (URL u : urls) {
                    if (!Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    /**
     * 注册url，简单将url添加到已注册的列表{@link #register(URL)}
     *
     * @param url 注册信息
     * @see Registry#register(URL)
     */
    public void register(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Register: " + url);
        }
        registered.add(url);
    }

    public void unregister(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unregister: " + url);
        }
        registered.remove(url);
    }

    /**
     * 订阅:绑定监听者和受订阅url之间的关系
     * <ul>
     * <li>检验入参</li><br/>
     * <li>将监听者，加入受订阅的url其对应的监听者集合中</li><br/>
     * </ul>
     * <p>
     * tip:缓存关系由{@link #subscribed}维护
     * </p>
     *
     * @param url      订阅条件，不允许为空，如：consumer://10.20.153.10/com.alibaba.foo.BarService?version=1.0.0&application=kylin
     * @param listener 变更事件监听器，不允许为空
     */
    public void subscribe(URL url, NotifyListener listener) {
        //检验入参
        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subscribe: " + url);
        }
        //缓存操作。对于目录服务持有的相关的订阅集合，其一个受订阅的url会有多个监听者进行对我们的监听
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners == null) {
            subscribed.putIfAbsent(url, new ConcurrentHashSet<NotifyListener>());
            listeners = subscribed.get(url);
        }
        listeners.add(listener);
    }

    public void unsubscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unsubscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }

    protected void recover() throws Exception {
        // register
        Set<URL> recoverRegistered = new HashSet<URL>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                register(url);
            }
        }
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    subscribe(url, listener);
                }
            }
        }
    }

    /**
     * 简单检验，包装返回的值总是有值的
     *
     * @param url
     * @param urls
     * @return
     */
    protected static List<URL> filterEmpty(URL url, List<URL> urls) {
        if (urls == null || urls.size() == 0) {
            List<URL> result = new ArrayList<URL>(1);
            result.add(url.setProtocol(Constants.EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    /**
     * 通知注册事件
     * <ul>
     * <li>检验整个集群urls合法性</li><br/>
     * <li>遍历{@link #subscribed}，寻找能与备用urls匹配的映射元素</li><br/>
     * <li>遍历元素的所有订阅者，尝试通知</li><br/>
     * </ul>
     *
     * @param urls 注册中心的集群地址url列表，不是单纯的备机地址
     * @see #notify(URL, NotifyListener, List)
     */
    protected void notify(List<URL> urls) {

        if (urls == null || urls.isEmpty()) return;

        //遍历所有的监听者，对已经受订阅的相关url进行通知。
        //通知订阅了相关的url（本集群的url)进行处理
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {

            URL url = entry.getKey();

            //对于不匹配的直接忽略掉，只需要和集群地址的第一个也就是主地址进行匹配就可以了
            //这里是注册事件，对于url列表来说，其path为RegistryService，其interface为RegistryService
            //因为urls集群除了host以及port可能存在的不同之外，其他的信息是全部相同的。
            if (!UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }

            //对于匹配的url。则需要进行通知
            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        logger.error("Failed to notify registry event, urls: " + urls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    /**
     * 通知事件
     * <ul>
     * <li>检验传入参数</li><br/>
     * <li>遍历所有urls，寻找与url匹配的元素，获取元素中键为{@link Constants#CATEGORY_KEY}的值，默认{@link Constants#DEFAULT_CATEGORY}作为分组依据</li><br/>
     * <li>合并相关信息到{@link #notified}</li><br/>
     * <li>分组通知所有能和受订阅的url匹配上的url</li><br/>
     * </ul>
     *
     * @param url      受订阅的url
     * @param listener url的订阅者
     * @param urls     候选的url列表，其中可能存在元素和受订阅的url匹配
     * @see #saveProperties(URL)
     * @see NotifyListener#notify(List)；
     */
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        //入参的检查
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((urls == null || urls.size() == 0)
                && !Constants.ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }

        //队候选的url列表进行分组匹配，分组依据u.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY)一致
        //key为目录的信息，value为对应目录的元信息列表集合
        //不匹配的忽略的掉
        //对传递进来的参数urls列表进行简单分组，由于是方法调用，面向的场景很多，也就无法保证该urls列表和url就是适配过的，因此这里还要进行一次适配。
        Map<String, List<URL>> result = new HashMap<String, List<URL>>();
        for (URL u : urls) {
            //对元素进行适配，不匹配的就不处理了，在匹配的情况下，根据目录进行不同分类。
            if (UrlUtils.isMatch(url, u)) {
                String category = u.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
                List<URL> categoryList = result.get(category);
                if (categoryList == null) {
                    categoryList = new ArrayList<URL>();
                    result.put(category, categoryList);
                }
                categoryList.add(u);
            }
        }
        //没有则直接返回
        if (result.size() == 0) {
            return;
        }
        //合并上面的分组信息到notified

        //尝试获得依据分组的缓存，一个url其可以有多个组。
        //没有则新建
        Map<String, List<URL>> categoryNotified = notified.get(url);
        if (categoryNotified == null) {
            notified.putIfAbsent(url, new ConcurrentHashMap<String, List<URL>>());
            categoryNotified = notified.get(url);
        }
        //将上面的result信息进行放入缓存中，放入分组信息，分组进行通知
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            // 分组通知时，尝试先写入缓存，保证注册中心订阅失败时，使用缓存进行通知
            // 保存相关属性，将相关信息写入文件，url和其信息
            saveProperties(url);
            //分组通知信息
            listener.notify(categoryList);
        }
    }

    /**
     * 保存URL中的信息
     * <ul>
     * <li>从缓存中找到相应的组，对其URL的列表进行字符串上的合并 使用分割符{@link #URL_SEPARATOR}</li><br/>
     * <li>设置进缓存properties中，key为url对应的serviceKey，value为以上说明的字符串</li><br/>
     * <li>记录变化的次数{@link #lastCacheChanged}</li>
     * <li>根据同步标识{@link #syncSaveFile} 进行同步操作</li><br/>
     * </ul>
     *
     * @param url 需要保存的URL
     * @see #doSaveProperties(long)
     */
    private void saveProperties(URL url) {
        // 文件为null直接返回
        if (file == null) {
            return;
        }

        try {
            StringBuilder buf = new StringBuilder();
            Map<String, List<URL>> categoryNotified = notified.get(url);
            if (categoryNotified != null) {
                //遍历值，不区分相关组别
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }
            // 保存对应的url和该url的信息
            properties.setProperty(url.getServiceKey(), buf.toString());
            // 自动+1
            long version = lastCacheChanged.incrementAndGet();
            //是否异步进行
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                registryCacheExecutor.execute(new SaveProperties(version));
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    public void destroy() {
        if (logger.isInfoEnabled()) {
            logger.info("Destroy registry:" + getUrl());
        }
        Set<URL> destroyRegistered = new HashSet<URL>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<URL>(getRegistered())) {
                if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                    try {
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    public String toString() {
        return getUrl().toString();
    }

}