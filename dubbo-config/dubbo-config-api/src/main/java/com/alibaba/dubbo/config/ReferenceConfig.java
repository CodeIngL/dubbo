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
package com.alibaba.dubbo.config;

import java.io.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.dubbo.config.support.Parameter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.StaticContext;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.directory.StaticDirectory;
import com.alibaba.dubbo.rpc.cluster.support.AvailableCluster;
import com.alibaba.dubbo.rpc.cluster.support.ClusterUtils;
import com.alibaba.dubbo.rpc.protocol.injvm.InjvmProtocol;
import com.alibaba.dubbo.rpc.service.GenericService;
import com.alibaba.dubbo.rpc.support.ProtocolUtils;

/**
 * <p>
 * ReferenceConfig 服务引用类（消费方）
 * </p>
 * <p>
 * 该类通过持有一个接口来实现对服务方提供的服务(包括自己本身作为服务方，以及多服务提供方形成集群)的引用<br/>
 * 两种基本方式：
 * <ul>
 * <li>对内引用</li><br/>
 * <li>对外引用</li><br/>
 * </ul>
 * </p>
 *
 * @author william.liangf
 */
public class ReferenceConfig<T> extends AbstractReferenceConfig {

    private static final long serialVersionUID = -5864351140409987595L;

    // Protocl$Adaptive单例唯一,
    // 加载时刻，类载入jvm后
    private static final Protocol refprotocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    // Cluster$Adaptive单例唯一,
    // 加载时刻，类载入jvm后
    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    // proxyFactory$Adaptive单例唯一,
    // 加载时刻，类载入jvm后
    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    // 接口类型，用于表明服务引用的接口全类名
    private String interfaceName;

    // 接口类名，用于表明服务引用的接口类类型
    // 对于泛化调用。其为:GenericService
    private Class<?> interfaceClass;

    // 客户端类型，
    private String client;

    // 点对点直连服务提供地址(not only 点对点)
    // 该配置是最高优先级配置，一旦有配置，会忽略配置的相关注册中心配置类
    private String url;

    // 方法配置类集合，接口方法对应的相关配置
    private List<MethodConfig> methods;

    // 缺省配置(ReferenceConfig对应的模板配置类)
    private ConsumerConfig consumer;

    // 协议
    private String protocol;

    // 接口代理类引用，代表了能实现引用的接口实现
    private transient volatile T ref;

    // 持有的最外层网络的执行对象（invoker）
    private transient volatile Invoker<?> invoker;

    // 初始化标记
    private transient volatile boolean initialized;

    // 销毁标记
    private transient volatile boolean destroyed;

    //远程or本地地址集合(元信息集合)
    private final List<URL> urls = new ArrayList<URL>();

    @SuppressWarnings("unused")
    private final Object finalizerGuardian = new Object() {
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            if (!ReferenceConfig.this.destroyed) {
                logger.warn("ReferenceConfig(" + url + ") is not DESTROYED when FINALIZE");
            }
        }
    };

    public ReferenceConfig() {
    }

    public ReferenceConfig(Reference reference) {
        appendAnnotation(Reference.class, reference);
    }

    public URL toUrl() {
        return urls == null || urls.size() == 0 ? null : urls.iterator().next();
    }

    public List<URL> toUrls() {
        return urls;
    }

    /**
     * 服务引用的入口。<br/>
     * 获得配置接口对应的服务引用实现。<br/>
     * tip：一旦销毁就不能引用
     *
     * @return rpc接口的代理, 包装了网络细节
     * @see #init()
     */
    public synchronized T get() {
        if (destroyed) {
            throw new IllegalStateException("Already destroyed!");
        }
        //ref==null是很粗略的检查是否已经存在引用了，因为init是一个重量级操作，时间跨度比较大。另一个属性initialized来避免竞争
        if (ref == null) {
            init();
        }
        return ref;
    }

    /**
     * 对远程引用对象进行销毁
     */
    public synchronized void destroy() {
        if (ref == null) {
            return;
        }
        if (destroyed) {
            return;
        }
        destroyed = true;
        try {
            invoker.destroy();
        } catch (Throwable t) {
            logger.warn("Unexpected err when destroy invoker of ReferenceConfig(" + url + ").", t);
        }
        invoker = null;
        ref = null;
    }

    /**
     * 服务引用入口(重量操作)
     * <p>
     * <ul>
     * <li>对配置属性进行校验，对元信息{@link URL}进行信息生成</li><br/>
     * <li>实现服务引用获得{@link Invoker}</li><br/>
     * <li>包装{@link Invoker}获得相应代理</li><br/>
     * </ul>
     * @see #createProxy
     */
    private void init() {
        // 校验初始化标志(对于已经初始化过的，不再继续逻辑处理)，防止并发多初始化，
        if (initialized) {
            return;
        }
        initialized = true;

        // 检验接口名(接口名是必填的配置项)
        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
        }

        // 获取模板配置类全局配置(consumer代表的各类ReferenceConfig的模板)
        checkConsumer();

        // 尝试对引用(本身)配置类完成基本属性的填充
        appendProperties(this);

        // 泛接口不存在尝试使用模板配置类来获得
        if (generic == null) {
            if (consumer != null) {
                setGeneric(consumer.getGeneric());
            }
        }

        //对是泛接口的处理
        if (ProtocolUtils.isGeneric(generic)) {
            interfaceClass = GenericService.class;
        } else {
            // 使用反射获得接口名
            try {
                interfaceClass = Class.forName(interfaceName, true, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            checkInterfaceAndMethods(interfaceClass, methods);
        }

        // 尝试获得该接口对应的元信息配置
        // 1. 从系统属性中获得
        // 2. 从文件中获得---文件路径从系统属性中获得，否则使用默认文件
        String resolve = System.getProperty(interfaceName);
        String resolveFile = null;
        if (resolve == null || resolve.length() == 0) {
            resolveFile = System.getProperty("dubbo.resolve.file");
            if (resolveFile == null || resolveFile.length() == 0) {
                File userResolveFile = new File(new File(System.getProperty("user.home")), "dubbo-resolve.properties");
                if (userResolveFile.exists()) {
                    resolveFile = userResolveFile.getAbsolutePath();
                }
            }
            if (resolveFile != null && resolveFile.length() > 0) {
                Properties properties = new Properties();
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(new File(resolveFile));
                    properties.load(fis);
                } catch (IOException e) {
                    throw new IllegalStateException("Unload " + resolveFile + ", cause: " + e.getMessage(), e);
                } finally {
                    try {
                        if (null != fis) fis.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
                resolve = properties.getProperty(interfaceName);
            }
        }

        // 根据接口对应的元信息配置获得不同的方式打印不同的日志提示信息
        if (resolve != null && resolve.length() > 0) {
            url = resolve;
            if (logger.isWarnEnabled()) {
                if (resolveFile != null && resolveFile.length() > 0) {
                    logger.warn("Using default dubbo resolve file " + resolveFile + " replace " + interfaceName + "" + resolve + " to p2p invoke remote service.");
                } else {
                    logger.warn("Using -D" + interfaceName + "=" + resolve + " to p2p invoke remote service.");
                }
            }
        }
        checkApplication();

        //对应ReferenceConfig不存在相关配置类，尝试使用模板配置类完成默认值的设定
        if (consumer != null) {
            if (application == null) {
                application = consumer.getApplication();
            }
            if (module == null) {
                module = consumer.getModule();
            }
            if (registries == null) {
                registries = consumer.getRegistries();
            }
            if (monitor == null) {
                monitor = consumer.getMonitor();
            }
        }
        if (module != null) {
            if (registries == null) {
                registries = module.getRegistries();
            }
            if (monitor == null) {
                monitor = module.getMonitor();
            }
        }
        if (application != null) {
            if (registries == null) {
                registries = application.getRegistries();
            }
            if (monitor == null) {
                monitor = application.getMonitor();
            }
        }
        checkStubAndMock(interfaceClass);

        //构建本接口对应元信息url的剩下参数信息map。
        Map<String, String> map = new HashMap<String, String>();
        map.put(Constants.SIDE_KEY, Constants.CONSUMER_SIDE);
        map.put(Constants.DUBBO_VERSION_KEY, Version.getVersion());
        map.put(Constants.TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(Constants.PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
        if (!isGeneric()) {
            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put("revision", revision);
            }
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if (methods.length == 0) {
                logger.warn("NO method found in service interface " + interfaceClass.getName());
                map.put("methods", Constants.ANY_VALUE);
            } else {
                map.put("methods", StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }
        map.put(Constants.INTERFACE_KEY, interfaceName);
        //复杂的参数信息的构造
        appendParameters(map, application);
        appendParameters(map, module);
        appendParameters(map, consumer, Constants.DEFAULT_KEY);
        appendParameters(map, this);
        // 获得前缀，也就是使用接口信息区分其他接口引用，由group，interfaceName，version组成
        String prefix = StringUtils.getServiceKey(map);
        Map<Object, Object> attributes = new HashMap<Object, Object>();
        if (methods != null && methods.size() > 0) {
            for (MethodConfig method : methods) {
                //这里的前缀没有加接口信息的原因是该map最后作为接口对应url的参数信息，url已经能区分不同接口
                appendParameters(map, method, method.getName());
                String retryKey = method.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        map.put(method.getName() + ".retries", "0");
                    }
                }
                //这里加上了前缀的原因是该attribute会在线程上下文传递，需要区分不同接口的方法
                appendAttributes(attributes, method, prefix + "." + method.getName());
                //处理其他的的配置需要加入attribute从而在线程上下文中可见
                checkAndConvertImplicitConfig(method, map, attributes);
            }
        }
        //attributes通过系统context进行存储.
        StaticContext.getSystemContext().putAll(attributes);
        //创建代理
        ref = createProxy(map);
    }

    /**
     *
     * @param method 方法配置类
     * @param map 接口引用的相关参数
     * @param attributes 暴露给线程上下文的属性
     */
    private static void checkAndConvertImplicitConfig(MethodConfig method, Map<String, String> map, Map<Object, Object> attributes) {
        //check config conflict
        // 检查配置
        if (Boolean.FALSE.equals(method.isReturn()) && (method.getOnreturn() != null || method.getOnthrow() != null)) {
            throw new IllegalStateException("method config error : return attribute must be set true when onreturn or onthrow has been setted.");
        }
        //convert onreturn methodName to Method
        //获得关键的key
        String onReturnMethodKey = StaticContext.getKey(map, method.getName(), Constants.ON_RETURN_METHOD_KEY);
        //获得attribute中已有的信息
        Object onReturnMethod = attributes.get(onReturnMethodKey);
        if (onReturnMethod != null && onReturnMethod instanceof String) {
            //已有信息，是String字符串,需要做转换
            //转换放入信息信息，将字符串转换对应的方法
            attributes.put(onReturnMethodKey, getMethodByName(method.getOnreturn().getClass(), onReturnMethod.toString()));
        }
        //convert onthrow methodName to Method
        //同上
        String onThrowMethodKey = StaticContext.getKey(map, method.getName(), Constants.ON_THROW_METHOD_KEY);
        Object onThrowMethod = attributes.get(onThrowMethodKey);
        if (onThrowMethod != null && onThrowMethod instanceof String) {
            attributes.put(onThrowMethodKey, getMethodByName(method.getOnthrow().getClass(), onThrowMethod.toString()));
        }
        //convert oninvoke methodName to Method
        //同上
        String onInvokeMethodKey = StaticContext.getKey(map, method.getName(), Constants.ON_INVOKE_METHOD_KEY);
        Object onInvokeMethod = attributes.get(onInvokeMethodKey);
        if (onInvokeMethod != null && onInvokeMethod instanceof String) {
            attributes.put(onInvokeMethodKey, getMethodByName(method.getOninvoke().getClass(), onInvokeMethod.toString()));
        }
    }

    private static Method getMethodByName(Class<?> clazz, String methodName) {
        try {
            return ReflectUtils.findMethodByMethodName(clazz, methodName);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 创建代理
     * 根据本身的配置，或者元信息中的配置信息，完成不同方式的引用
     *
     * @param map 元信息中的参数配置信息
     * @return 具体的代理实现
     */
    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    private T createProxy(Map<String, String> map) {

        //1.对（injvm）标记进行处理
        final boolean isJvmRefer;
        if (injvm != null) {
            isJvmRefer = injvm.booleanValue();
        } else {
            if (url != null && url.length() > 0) { //指定URL的情况下，不做本地引用
                isJvmRefer = false;
            } else {
                isJvmRefer = InjvmProtocol.getInjvmProtocol().isInjvmRefer(new URL("temp", "localhost", 0, map));
            }
        }

        //2.对（isJvmRefer临时变量）进行处理
        if (isJvmRefer) {
            //对内引用
            URL url = new URL(Constants.LOCAL_PROTOCOL, NetUtils.LOCALHOST, 0, interfaceClass.getName()).addParameters(map);
            invoker = refprotocol.refer(interfaceClass, url);
            if (logger.isInfoEnabled()) {
                logger.info("Using injvm service " + interfaceClass.getName());
            }
        } else {
            //对外引用
            if (url != null && url.length() > 0) { // 用户指定URL，指定的URL可能是对点对直连地址，也可能是注册中心URL
                String[] us = Constants.SEMICOLON_SPLIT_PATTERN.split(url);
                for (String u : us) {
                    URL url = URL.valueOf(u);
                    if (url.getPath() == null || url.getPath().length() == 0) {
                        url = url.setPath(interfaceName);
                    }
                    if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                        URL monitorUrl = loadMonitor(url);
                        if (monitorUrl != null) {
                            map.put(Constants.MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
                        }
                        urls.add(url.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                    } else {
                        urls.add(ClusterUtils.mergeUrl(url, map));
                    }
                }
            } else {
                // 通过注册中心配置拼装URL
                List<URL> us = loadRegistries(false);
                if (us != null && us.size() > 0) {
                    for (URL u : us) {
                        URL monitorUrl = loadMonitor(u);
                        if (monitorUrl != null) {
                            map.put(Constants.MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
                        }
                        urls.add(u.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                    }
                }
                if (urls == null || urls.size() == 0) {
                    throw new IllegalStateException("No such any registry to reference " + interfaceName + " on the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", please config <dubbo:registry address=\"...\" /> to your spring config.");
                }
            }
            //多种策略共存只能通过设置字符串url，否则全部都是使用注册中心，注册中心的支持决定了其范围
            if (urls.size() == 1) {
                //urls.length = 1 的情况下无论是注册中心，还是其他url，总是能暴露出来，也不需要统一的cluster取分发
                //这个是最寻常的角色，一般开发者，不会配置字符串url，只会单独配置一个注册中心，也就是urls.size为1的情况
                invoker = refprotocol.refer(interfaceClass, urls.get(0));
            } else {
                //urls.length > 1 的情况下存在多种url，需要使用统一的门户完成对外要求
                //构建的原因也很简单，这一个消费接口需要引用多个地方来保证高可用性，等等
                //一个接口消费引用多个地方，涉及到调用时选择其中之一进行消费了
                //两种可能:使用字符串url获得的（多种形式）；使用xml注册中心获得的（全部都是注册中心url）
                List<Invoker<?>> invokers = new ArrayList<Invoker<?>>();
                URL registryURL = null;
                for (URL url : urls) {
                    invokers.add(refprotocol.refer(interfaceClass, url));
                    if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                        registryURL = url; // 用了最后一个registry url
                    }
                }
                if (registryURL != null) {
                    // 对有注册中心的Cluster 只用 AvailableCluster
                    URL u = registryURL.addParameter(Constants.CLUSTER_KEY, AvailableCluster.NAME);
                    invoker = cluster.join(new StaticDirectory(u, invokers));
                } else {
                    // 只能由配置字符串url构建的多个普通的url，没有使用注册中心，
                    // 同时配置文件中，没有配置相关的注册中心的标签，spring情况下
                    invoker = cluster.join(new StaticDirectory(invokers));
                }
            }
        }

        //3.对 (check)标记进行处理
        Boolean c = check;
        if (c == null && consumer != null) {
            c = consumer.isCheck();
        }
        //默认是true，即会检测invoker的可用性
        if (c == null) {
            c = true; // default true
        }
        if (c && !invoker.isAvailable()) {
            throw new IllegalStateException("Failed to check the status of the service " + interfaceName + ". No provider available for the service " + (group == null ? "" : group + "/") + interfaceName + (version == null ? "" : ":" + version) + " from the url " + invoker.getUrl() + " to the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());
        }
        if (logger.isInfoEnabled()) {
            logger.info("Refer dubbo service " + interfaceClass.getName() + " from url " + invoker.getUrl());
        }

        //4.创建服务代理
        return (T) proxyFactory.getProxy(invoker);
    }

    /**
     * 检验消费配置类(可选)，无则新建
     * 使用{@link #appendProperties(AbstractConfig)}完成配置类基本属性填充
     *
     * @see #appendProperties(AbstractConfig)
     */
    private void checkConsumer() {
        if (consumer == null) {
            consumer = new ConsumerConfig();
        }
        appendProperties(consumer);
    }

    public Class<?> getInterfaceClass() {
        if (interfaceClass != null) {
            return interfaceClass;
        }
        if (isGeneric()
                || (getConsumer() != null && getConsumer().isGeneric())) {
            return GenericService.class;
        }
        try {
            if (interfaceName != null && interfaceName.length() > 0) {
                this.interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
            }
        } catch (ClassNotFoundException t) {
            throw new IllegalStateException(t.getMessage(), t);
        }
        return interfaceClass;
    }

    /**
     * @param interfaceClass
     * @see #setInterface(Class)
     * @deprecated
     */
    @Deprecated
    public void setInterfaceClass(Class<?> interfaceClass) {
        setInterface(interfaceClass);
    }

    public String getInterface() {
        return interfaceName;
    }

    public void setInterface(String interfaceName) {
        this.interfaceName = interfaceName;
        if (id == null || id.length() == 0) {
            id = interfaceName;
        }
    }

    public void setInterface(Class<?> interfaceClass) {
        if (interfaceClass != null && !interfaceClass.isInterface()) {
            throw new IllegalStateException("The interface class " + interfaceClass + " is not a interface!");
        }
        this.interfaceClass = interfaceClass;
        setInterface(interfaceClass == null ? (String) null : interfaceClass.getName());
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        checkName("client", client);
        this.client = client;
    }

    @Parameter(excluded = true)
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<MethodConfig> getMethods() {
        return methods;
    }

    @SuppressWarnings("unchecked")
    public void setMethods(List<? extends MethodConfig> methods) {
        this.methods = (List<MethodConfig>) methods;
    }

    public ConsumerConfig getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerConfig consumer) {
        this.consumer = consumer;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    // just for test
    Invoker<?> getInvoker() {
        return invoker;
    }

    public static void main(String[] args) {
        String[] methods = Wrapper.getWrapper(GenericService.class).getMethodNames();
        System.out.println(methods.length);
        System.out.println(methods[0]);
        Wrapper wrapper =  Wrapper.getWrapper(Serializable.class);
        methods = wrapper.getMethodNames();
        System.out.println(methods.length);
        System.out.println(methods[0]);
    }
}