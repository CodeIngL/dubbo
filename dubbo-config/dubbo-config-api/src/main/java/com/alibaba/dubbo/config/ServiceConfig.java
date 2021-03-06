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

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.ClassHelper;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.dubbo.config.support.Parameter;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.cluster.ConfiguratorFactory;
import com.alibaba.dubbo.rpc.service.GenericService;
import com.alibaba.dubbo.rpc.support.ProtocolUtils;

import static com.alibaba.dubbo.common.Constants.*;
import static com.alibaba.dubbo.common.utils.NetUtils.LOCALHOST;
import static com.alibaba.dubbo.common.utils.StringUtils.isEmpty;
import static com.alibaba.dubbo.common.utils.StringUtils.isNotEmpty;

/**
 * ServiceConfig
 *
 * @author william.liangf
 * @export
 */
public class ServiceConfig<T> extends AbstractServiceConfig {

    private static final long serialVersionUID = 3033787999037024738L;

    //Protocl$Adaptive单例唯一,
    //加载时刻，类载入jvm后
    private static final Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    //ProxyFactory$Adaptive单例唯一
    //加载时刻，类载入jvm后
    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    //
    //加载时刻，类载入jvm后
    private static final Map<String, Integer> RANDOM_PORT_MAP = new HashMap<String, Integer>();

    //------------------
    //应用相关
    //------------------


    // 接口类型
    private String interfaceName;

    // 接口类型
    private Class<?> interfaceClass;

    // 具体实现类引用
    private T ref;

    // 服务名称
    private String path;

    // 方法配置
    private List<MethodConfig> methods;

    //服务提供者的抽象
    private ProviderConfig provider;

    //元信息列表
    private final List<URL> urls = new ArrayList<URL>();

    //导出列表
    private final List<Exporter<?>> exporters = new ArrayList<Exporter<?>>();

    //导出标志
    private transient volatile boolean exported;

    //未导出标志
    private transient volatile boolean unexported;

    //是否是通用接口标志
    private volatile String generic;

    public ServiceConfig() {
    }

    public ServiceConfig(Service service) {
        appendAnnotation(Service.class, service);
    }

    public URL toUrl() {
        return urls == null || urls.size() == 0 ? null : urls.iterator().next();
    }

    public List<URL> toUrls() {
        return urls;
    }

    /**
     * 该项排除
     *
     * @return 是否已经导出
     * @see #appendProperties(AbstractConfig)
     */
    @Parameter(excluded = true)
    public boolean isExported() {
        return exported;
    }

    /**
     * 该项排除
     *
     * @return 是否为导出
     * @see #appendProperties(AbstractConfig)
     */
    @Parameter(excluded = true)
    public boolean isUnexported() {
        return unexported;
    }

    /**
     * 服务导出入口
     */
    public synchronized void export() {
        if (export == null && provider != null) {//设置导出标志
            export = provider.getExport();
        }
        if (export != null && !export) {//检查导出标志
            return;
        }
        if (delay == null && provider != null) {//设置延迟导出
            delay = provider.getDelay();
        }
        if (delay != null && delay > 0) {//对延迟导出处理，delay有值，使用线程:DelayExportServiceThread延迟delay毫秒后，异步导出
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(delay);
                    } catch (Throwable e) {
                    }
                    //延迟导出
                    doExport();
                }
            });
            thread.setDaemon(true);
            thread.setName("DelayExportServiceThread");
            thread.start();
            return;
        }
        doExport();//立即导出
    }

    /**
     * 服务导出导出实际处理逻辑
     */
    protected synchronized void doExport() {
        if (unexported) {//检查未导出标志
            throw new IllegalStateException("Already unexported!");
        }
        if (exported) {//检查导出标志,
            return;
        }
        exported = true;//导出了提前设置导出标记

        //设置基本的属性，通过遍历set方法实现，基本类型属性的设置
        //多个配置区分的类为其id;
        appendProperties(this);
        //检查接口名字，必填
        checkInterfaceName();
        //检查引用是否实现接口具体对象
        checkRef();
        //检查通用属性
        checkGeneric();
        //检验,设置属性
        checkStubAndMock(interfaceClass);
        //设置上下文路径
        checkPath();
        //检查默认情况，设置基本数据
        checkProvider();
        //冗余相关属性，服务类属性和提供者一致，都是复杂类型
        checkModule();
        //检验,设置属性
        checkApplication();
        //检查接口和方法，接口类，是否都包含相关method的方法
        checkInterfaceAndMethods(interfaceClass, methods);
        //检验,设置属性
        checkProtocol();
        //导出url
        doExportUrls();
    }

    private void checkPath() {
        //设置path，url的应用路径部分
        if (path == null || path.length() == 0) {
            path = interfaceName;
        }
    }

    /**
     * 设置通用接口属性标志
     * <p>
     * 也就是泛化调用的场景
     * </p>
     */
    private void checkGeneric() {
        if (ref instanceof GenericService) {
            generic = Boolean.TRUE.toString();
            return;
        }
        generic = Boolean.FALSE.toString();
    }

    private void checkInterfaceName() {
        if (interfaceName == null || interfaceName.length() == 0) {
            throw new IllegalStateException("<dubbo:service interface=\"\" /> interface not allow null!");
        }
    }

    protected void checkInterfaceAndMethods(Class<?> interfaceClass, List<MethodConfig> methods) {
        if (ref instanceof GenericService) {
            return;
        }
        super.checkInterfaceAndMethods(interfaceClass, methods);
    }

    private void checkModule() {
        if (module == null) {
            return;
        }
        if (registries == null) {
            registries = module.getRegistries();
        }
        if (monitor == null) {
            monitor = module.getMonitor();
        }
    }

    /**
     * 检查Ref和interfaceClass的一致性
     * <p>
     * ref如果是特殊的接口GenericService,则说明是泛化调用，这是一个特殊的场景
     * </p>
     * <p>
     * 其他情况下:Ref必须是interfaceClass的实现类
     * </p>
     */
    private void checkRef() {
        try {
            // 检查引用不为空，并且引用必需实现接口
            if (ref == null) {
                throw new IllegalStateException("ref not allow null!");
            }
            // 如果ref代表通用接口,那么interfaceClass属性就为GenericService.class
            if (ref instanceof GenericService) {
                interfaceClass = GenericService.class;
                return;
            }
            // 使用interface的名字来获得interfaceClass
            interfaceClass = Class.forName(interfaceName, true, Thread.currentThread().getContextClassLoader());
            if (interfaceClass.isInstance(ref)) {
                return;
            }
            throw new IllegalStateException("The class " + ref.getClass().getName() + " unimplemented interface " + interfaceClass + "!");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 结束导出
     */
    public synchronized void unexport() {
        if (!exported) {
            return;
        }
        if (unexported) {
            return;
        }
        if (exporters != null && exporters.size() > 0) {
            for (Exporter<?> exporter : exporters) {
                try {
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn("unexpected err when unexport" + exporter, t);
                }
            }
            exporters.clear();
        }
        unexported = true;
    }

    /**
     * 导出
     * <ul>
     * <li>首先根据注册配置类获得URL</li>
     * <li>根据协议配置类和URL进行处理</li>
     * </ul>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void doExportUrls() {
        List<URL> registryURLs = loadRegistries(true);
        //对每种协议都进行导出
        for (ProtocolConfig protocolConfig : protocols) {
            doExportUrlsFor1Protocol(protocolConfig, registryURLs);
        }
    }

    /**
     * 根据协议配置类和URL进行导出
     * <ul>
     * <li>协议默认配置名是dubbo</li>
     * <li>尝试使用协议配置类中的host熟悉，否则尝试使用provider配置类中的host</li>
     * <li>检验地址是否是本机地址，尝试ping通URL的地址</li>
     * <li>尝试使用协议配置类中的port熟悉，否则尝试使用provider配置类中的port</li>
     * <li>检验port的有效性，并在缓存中放置配置类(protocolConfig.name)名称和port的映射关系</li>
     * <li>生成元信息map
     * <ul>
     * <li>添加key:anyhost;value:true。如果地址是本机地址的情况下</li><br/>
     * <li>添加key:side;value:provider。说明这边是服务提供者</li><br/>
     * <li>添加key:side;value:provider。</li><br/>
     * <li>添加key:dubbo;value:2.0.0。</li><br/>
     * <li>添加key:dubbo;value:2.0.0。</li><br/>
     * <li>添加key:timestamp;value:${当前时间戳}</li><br/>
     * <li>添加key:pid;value:${当前时间戳}</li><br/>
     * <li>追加配置类application中的信息</li><br/>
     * <li>追加配置类module中的信息</li><br/>
     * <li>追加配置类provider中的信息这里会使用前缀default</li><br/>
     * <li>追加配置类protocolConfig的信息</li><br/>
     * <li>追加配置类本身的信息</li><br/>
     * <li>追加配置类method，前置method.getName</li><br/>
     * <li>尝试转换键值对，如果元信息map中含有method.getName（）+.retry如果该值是false，转换为method.getName.retries:0的存在存入map中</li><br/>
     * <li>追加配置类argument的处理，并置入元信息中</li><br/>
     * <li>对通用接口，放入（"generic",generic）,("methods",*)进行扩张</li><br/>
     * <li>放入（"revision",上一个版本），(methods,包装处理的),进行扩张</li><br/>
     * <li>添加key:token;value:${token}</li><br/>
     * <li>对配置类配置的是injvm的处理，该选项将 会不暴露，如果选择该选项，配置类的注册 标志会设定为false，并在元信息中追加notify：false的键值对</li><br/>
     * <li>尝试获得应用上下文，如果不存在的话尝试从配置类provider中获取相关上下文</li><br/>
     * <li>生成新的url，关于协议配置类，不是注册配置类的URL</li><br/>
     * <li>尝试获得ConfiguratorFactory上面关于url中协议名称的扩展，如果有的话，默认名称是dubbo.这里提供了扩展点外部人员用来处理url</li><br/>
     * <li>根据URL中的scope属性，进行不同方式的暴露</li><br/>
     * </ul>
     * </li>
     * </ul>
     *
     * @param protocolConfig
     * @param registryURLs
     */
    private void doExportUrlsFor1Protocol(ProtocolConfig protocolConfig, List<URL> registryURLs) {
        //默认是dubbo协议
        String protocolName = protocolConfig.getName();
        if (isEmpty(protocolName)) {
            protocolName = "dubbo";
        }

        //获取host
        String host = protocolConfig.getHost();
        if (isEmpty(host) && provider != null) {
            host = provider.getHost();
        }

        boolean anyhost = false;        //本机地址
        if (NetUtils.isInvalidLocalHost(host)) {
            anyhost = true;
            try {
                host = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                logger.warn(e.getMessage(), e);
            }
            if (NetUtils.isInvalidLocalHost(host)) {
                if (registryURLs != null && registryURLs.size() > 0) {
                    //本地访问下注册中心，确定本机使用的ip地址，同时确定下与注册中心的网络连通性
                    for (URL registryURL : registryURLs) {
                        try {
                            Socket socket = new Socket();
                            try {
                                //socket连接
                                SocketAddress addr = new InetSocketAddress(registryURL.getHost(), registryURL.getPort());
                                socket.connect(addr, 1000);
                                host = socket.getLocalAddress().getHostAddress();
                                break;
                            } finally {
                                try {
                                    socket.close();
                                } catch (Throwable e) {
                                }
                            }
                        } catch (Exception e) {
                            logger.warn(e.getMessage(), e);
                        }
                    }
                }
                if (NetUtils.isInvalidLocalHost(host)) {
                    host = NetUtils.getLocalHost();
                }
            }
        }

        //获取端口
        Integer port = protocolConfig.getPort();
        if ((port == null || port == 0) && provider != null) {
            port = provider.getPort();
        }
        //默认端口
        final int defaultPort = ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(protocolName).getDefaultPort();
        if (port == null || port == 0) {
            port = defaultPort;
        }
        if (port <= 0) {
            port = getRandomPort(protocolName);
            if (port < 0) {
                port = NetUtils.getAvailablePort(defaultPort);
                putRandomPort(protocolName, port);
            }
            logger.warn("Use random available port(" + port + ") for protocol " + protocolName);
        }

        //放置详细的信息，
        //map["side":"provider","dubbo":"2.0.0","":"timestamp","xxxxxxx"]
        Map<String, String> map = new HashMap<String, String>();
        if (anyhost) {
            map.put(ANYHOST_KEY, "true");//本机地址情况下
        }
        map.put(SIDE_KEY, PROVIDER_SIDE);
        map.put(DUBBO_VERSION_KEY, Version.getVersion());
        map.put(TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
        appendParameters(map, application);
        appendParameters(map, module);
        appendParameters(map, provider, DEFAULT_KEY);
        appendParameters(map, protocolConfig);
        appendParameters(map, this);

        if (methods != null && methods.size() > 0) { //方法配置类处理
            for (MethodConfig methodConfig : methods) {
                appendParameters(map, methodConfig, methodConfig.getName()); //添加方法相关的信息
                if ("false".equals(map.remove(methodConfig.getName() + ".retry"))) {//配置信息转换，对于配置xxx.retry:false转换为xxx.retries:0
                    map.put(methodConfig.getName() + ".retries", "0");
                }

                List<ArgumentConfig> arguments = methodConfig.getArguments();//方法配置中的关于相关的参数的配置信息。
                if (arguments == null || arguments.size() == 0) {
                    continue;
                }
                for (ArgumentConfig argument : arguments) {
                    int argIndex = argument.getIndex();
                    String argType = argument.getType();
                    if (isNotEmpty(argType)) {//参数配置中指定了该参数的type
                        for (Method method : interfaceClass.getMethods()) { //遍历封装服务，找到和方法配置类匹配的方法。匹配的先决条件是名字相等
                            String methodName = method.getName();
                            if (!methodName.equals(methodConfig.getName())) {
                                continue;
                            }
                            Class<?>[] argTypes = method.getParameterTypes();//获得匹配方法的参数类型数组
                            if (argIndex != -1) { //参数配置类中指定了这个参数在所在的index。
                                if (argTypes[argIndex].getName().equals(argType)) {//存在合理的匹配
                                    appendParameters(map, argument, methodConfig.getName() + "." + argIndex);
                                } else {
                                    throw new IllegalArgumentException("argument config error : the index attribute and type attribute not match :index :" + argIndex + ", type:" + argType);
                                }
                            } else {
                                boolean findMark = false;
                                for (int j = 0; j < argTypes.length; j++) { //只有type，没有index，只要参数type符合就都符合。
                                    if (argTypes[j].getName().equals(argType)) {
                                        findMark = true;
                                        appendParameters(map, argument, methodConfig.getName() + "." + j);
                                    }
                                }
                                if (!findMark) {
                                    throw new IllegalArgumentException("argument config error : type attribute not match : type:" + argType);
                                }
                            }
                        }
                    } else if (argIndex != -1) {//仅仅配置index选项
                        appendParameters(map, argument, methodConfig.getName() + "." + argIndex);
                    } else {
                        //非法的配置项
                        throw new IllegalArgumentException("argument config must set index or type attribute.eg: <dubbo:argument index='0' .../> or <dubbo:argument type=xxx .../>");
                    }
                }
            } // end of methods for
        }

        if (ProtocolUtils.isGeneric(generic)) {//通用接口
            map.put("generic", generic);
            map.put("methods", ANY_VALUE);
        } else {//一般的接口
            String revision = Version.getVersion(interfaceClass, version);//调整的版本信息
            if (isNotEmpty(revision)) {
                map.put("revision", revision);
            }
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();//包装一个代类型，并获得相关要暴露的方法的名字
            if (methods.length == 0) {
                logger.warn("NO method found in service interface " + interfaceClass.getName());
                map.put("methods", ANY_VALUE);
            } else {
                map.put("methods", StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }

        if (!ConfigUtils.isEmpty(token)) {//设置token
            if (ConfigUtils.isDefault(token)) {
                map.put("token", UUID.randomUUID().toString());
            } else {
                map.put("token", token);
            }
        }

        if ("injvm".equals(protocolName)) { //injvm是内部使用，不用远程
            protocolConfig.setRegister(false);
            map.put("notify", "false");
        }

        String contextPath = protocolConfig.getContextpath();// 设置上下文路径，作为另一层区分，如果有的话
        if (provider != null && isEmpty(contextPath)) {
            contextPath = provider.getContextpath();
        }
        URL url = new URL(protocolName, host, port, (isEmpty(contextPath) ? "" : contextPath + "/") + path, map);

        //获取协议对应的类
        if (ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).hasExtension(url.getProtocol())) {
            url = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getExtension(url.getProtocol()).getConfigurator(url).configure(url); //还可以修改这个url
        }
        //scope通常有三个可选项来选择，1是none不进行暴露，2是remote进行远程暴露，3是local进行本地暴露
        String scope = url.getParameter(SCOPE_KEY);
        if (SCOPE_NONE.equalsIgnoreCase(scope)) {
            this.urls.add(url);
            return;
        }
        if (!SCOPE_REMOTE.equalsIgnoreCase(scope)) {//本地暴露方式
            exportLocal(url);
            this.urls.add(url);
            return;
        }
        //远程暴露方式
        if (logger.isInfoEnabled()) {
            logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
        }
        if (registryURLs == null || registryURLs.size() == 0) {
            Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, url);//不使用注册中心，典型的方式是配置了injvm。或者是N/A。
            Exporter<?> exporter = protocol.export(invoker);
            exporters.add(exporter);
            this.urls.add(url);
            return;
        }
        if (!url.getParameter("register", true)) {
            Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, url);//不使用注册中心，典型的方式是配置了injvm。或者是N/A。
            Exporter<?> exporter = protocol.export(invoker);
            exporters.add(exporter);
            this.urls.add(url);
            return;
        }
        //如果配置了注册配置类，会得到相应的注册url，同时该配置还不能是injvm，对于injvm，register对应的值总是false，因此不会使用远程配置
        //支持多个注册类对应的url上，也就是不同注册中心，都支持注册相应的信息
        for (URL registryURL : registryURLs) {
            url = url.addParameterIfAbsent("dynamic", registryURL.getParameter("dynamic"));//尝试从注册中心url中copy键值对dynamic,用来说明是否是动态服务
            URL monitorUrl = loadMonitor(registryURL);//尝试从注册中心url中获得监控的地址url，用来启动监控
            if (monitorUrl != null) {
                url = url.addParameterAndEncoded(MONITOR_KEY, monitorUrl.toFullString());//将监控url转移到url的配置中
            }
            if (logger.isInfoEnabled()) {
                logger.info("Register dubbo service " + interfaceClass.getName() + " url " + url + " to registry " + registryURL);
            }
            //invoker目前是最为接近实际调用者的包装对象。通过export来为我们的invoker进一层层的包装。
            Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, registryURL.addParameterAndEncoded(EXPORT_KEY, url.toFullString()));//将协议配置url转移到注册中心的url配置中(key:export)。至此，注册中心url可以得到协议配置url已经监控url等等信息
            Exporter<?> exporter = protocol.export(invoker);
            exporters.add(exporter);
        }
        this.urls.add(url);
    }


    /**
     * 本地暴露
     *
     * @param url 元信息
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void exportLocal(URL url) {
        if (LOCAL_PROTOCOL.equalsIgnoreCase(url.getProtocol())) {
            return;
        }
        URL local = URL.valueOf(url.toFullString())
                .setProtocol(LOCAL_PROTOCOL)
                .setHost(LOCALHOST)
                .setPort(0);
        Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, local);
        Exporter<?> exporter = protocol.export(invoker);
        exporters.add(exporter);
        logger.info("Export dubbo service " + interfaceClass.getName() + " to local registry");
    }

    /**
     * 设置provider
     *
     * @see #appendProperties(AbstractConfig)
     */
    private void checkProvider() {
        //服务提供者没有配置，使用默认的配置
        if (provider == null) {
            provider = new ProviderConfig();
        }
        //设置服务提供者的相关属性
        appendProperties(provider);

        //嵌套配置类不存在尝试使用模板配置类
        if (module == null) {
            module = provider.getModule();
        }
        if (application == null) {
            application = provider.getApplication();
        }
        if (registries == null) {
            registries = provider.getRegistries();
        }
        if (monitor == null) {
            monitor = provider.getMonitor();
        }
        if (protocols == null || protocols.size() == 0) {
            protocols = provider.getProtocols();
        }
    }

    /**
     * 校验设置protocols
     *
     * @see #appendProperties(AbstractConfig)
     */
    private void checkProtocol() {
        // 兼容旧版本
        if (protocols == null || protocols.size() == 0) {
            setProtocol(new ProtocolConfig());
        }
        for (ProtocolConfig protocolConfig : protocols) {
            if (StringUtils.isEmpty(protocolConfig.getName())) {
                protocolConfig.setName("dubbo");
            }
            appendProperties(protocolConfig);
        }
    }

    public Class<?> getInterfaceClass() {
        if (interfaceClass != null) {
            return interfaceClass;
        }
        if (ref instanceof GenericService) {
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

    public T getRef() {
        return ref;
    }

    public void setRef(T ref) {
        this.ref = ref;
    }

    /**
     * 该项排除
     *
     * @return 上下文路径
     * @see #appendProperties(AbstractConfig)
     */
    @Parameter(excluded = true)
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        checkPathName("path", path);
        this.path = path;
    }

    public List<MethodConfig> getMethods() {
        return methods;
    }

    @SuppressWarnings("unchecked")
    public void setMethods(List<? extends MethodConfig> methods) {
        this.methods = (List<MethodConfig>) methods;
    }

    public ProviderConfig getProvider() {
        return provider;
    }

    public void setGeneric(String generic) {
        if (StringUtils.isEmpty(generic)) {
            return;
        }
        if (ProtocolUtils.isGeneric(generic)) {
            this.generic = generic;
        } else {
            throw new IllegalArgumentException("Unsupported generic type " + generic);
        }
    }

    public String getGeneric() {
        return generic;
    }

    public void setProvider(ProviderConfig provider) {
        this.provider = provider;
    }

    public List<URL> getExportedUrls() {
        return urls;
    }

    // ======== Deprecated ========

    /**
     * @deprecated Replace to getProtocols()
     */
    @Deprecated
    public List<ProviderConfig> getProviders() {
        return convertProtocolToProvider(protocols);
    }

    /**
     * @deprecated Replace to setProtocols()
     */
    @Deprecated
    public void setProviders(List<ProviderConfig> providers) {
        this.protocols = convertProviderToProtocol(providers);
    }

    @Deprecated
    private static final List<ProtocolConfig> convertProviderToProtocol(List<ProviderConfig> providers) {
        if (providers == null || providers.size() == 0) {
            return null;
        }
        List<ProtocolConfig> protocols = new ArrayList<ProtocolConfig>(providers.size());
        for (ProviderConfig provider : providers) {
            protocols.add(convertProviderToProtocol(provider));
        }
        return protocols;
    }

    @Deprecated
    private static final List<ProviderConfig> convertProtocolToProvider(List<ProtocolConfig> protocols) {
        if (protocols == null || protocols.size() == 0) {
            return null;
        }
        List<ProviderConfig> providers = new ArrayList<ProviderConfig>(protocols.size());
        for (ProtocolConfig provider : protocols) {
            providers.add(convertProtocolToProvider(provider));
        }
        return providers;
    }

    @Deprecated
    private static final ProtocolConfig convertProviderToProtocol(ProviderConfig provider) {
        ProtocolConfig protocol = new ProtocolConfig();
        protocol.setName(provider.getProtocol().getName());
        protocol.setServer(provider.getServer());
        protocol.setClient(provider.getClient());
        protocol.setCodec(provider.getCodec());
        protocol.setHost(provider.getHost());
        protocol.setPort(provider.getPort());
        protocol.setPath(provider.getPath());
        protocol.setPayload(provider.getPayload());
        protocol.setThreads(provider.getThreads());
        protocol.setParameters(provider.getParameters());
        return protocol;
    }

    @Deprecated
    private static final ProviderConfig convertProtocolToProvider(ProtocolConfig protocol) {
        ProviderConfig provider = new ProviderConfig();
        provider.setProtocol(protocol);
        provider.setServer(protocol.getServer());
        provider.setClient(protocol.getClient());
        provider.setCodec(protocol.getCodec());
        provider.setHost(protocol.getHost());
        provider.setPort(protocol.getPort());
        provider.setPath(protocol.getPath());
        provider.setPayload(protocol.getPayload());
        provider.setThreads(protocol.getThreads());
        provider.setParameters(protocol.getParameters());
        return provider;
    }

    private static Integer getRandomPort(String protocol) {
        protocol = protocol.toLowerCase();
        if (RANDOM_PORT_MAP.containsKey(protocol)) {
            return RANDOM_PORT_MAP.get(protocol);
        }
        return Integer.MIN_VALUE;
    }

    private static void putRandomPort(String protocol, Integer port) {
        protocol = protocol.toLowerCase();
        if (!RANDOM_PORT_MAP.containsKey(protocol)) {
            RANDOM_PORT_MAP.put(protocol, port);
        }
    }
}
