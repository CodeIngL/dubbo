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
import java.util.*;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.config.support.Parameter;
import com.alibaba.dubbo.monitor.MonitorFactory;
import com.alibaba.dubbo.monitor.MonitorService;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.InvokerListener;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.support.MockInvoker;

import static com.alibaba.dubbo.common.Constants.*;
import static com.alibaba.dubbo.common.Version.getVersion;
import static com.alibaba.dubbo.common.utils.ConfigUtils.getPid;
import static com.alibaba.dubbo.common.utils.StringUtils.isEmpty;
import static com.alibaba.dubbo.common.utils.StringUtils.isNotEmpty;
import static com.alibaba.dubbo.config.RegistryConfig.NO_AVAILABLE;
import static java.lang.System.currentTimeMillis;

/**
 * AbstractDefaultConfig
 *
 * @author william.liangf
 * @export
 */
public abstract class AbstractInterfaceConfig extends AbstractMethodConfig {

    private static final long serialVersionUID = -1559314110797223229L;

    // 服务接口的本地实现类名
    protected String local;

    // 服务接口的本地实现类名
    protected String stub;

    // 服务监控
    protected MonitorConfig monitor;

    // 代理类型
    protected String proxy;

    // 集群方式
    protected String cluster;

    // 过滤器
    protected String filter;

    // 监听器
    protected String listener;

    // 负责人
    protected String owner;

    // 连接数限制,0表示共享连接，否则为该服务独享连接数
    protected Integer connections;

    // 连接数限制
    protected String layer;

    // 应用信息
    protected ApplicationConfig application;

    // 模块信息
    protected ModuleConfig module;

    // 注册中心
    protected List<RegistryConfig> registries;

    // callback实例个数限制
    private Integer callbacks;

    // 连接事件
    protected String onconnect;

    // 断开事件
    protected String ondisconnect;

    // 服务暴露或引用的scope,如果为local，则表示只在当前JVM内查找.
    private String scope;

    /**
     * 校验设置registries
     */
    protected void checkRegistry() {
        // 兼容旧版本
        if (registries == null || registries.size() == 0) {
            String address = ConfigUtils.getProperty("dubbo.registry.address");
            if (isNotEmpty(address)) {
                registries = new ArrayList<RegistryConfig>();
                for (String a : address.split("\\s*[|]+\\s*")) {
                    registries.add(new RegistryConfig(a));
                }
            }
        }
        if ((registries == null || registries.size() == 0)) {
            throw new IllegalStateException((getClass().getSimpleName().startsWith("Reference")
                    ? "No such any registry to refer service in consumer "
                    : "No such any registry to export service in provider ")
                    + NetUtils.getLocalHost()
                    + " use dubbo version "
                    + getVersion()
                    + ", Please add <dubbo:registry address=\"...\" /> to your spring config. If you want unregister, please set <dubbo:service registry=\"N/A\" />");
        }
        for (RegistryConfig registryConfig : registries) {
            appendProperties(registryConfig);
        }
    }

    /**
     * 校验设置application
     */
    /**
     * <p>
     * 校验设置application
     * <ul>
     * <li>不存在，尝试生成，前提在系统配置中能够找到key为的dubbo.application.name</li></br>
     * <li>依旧不存在，抛出异常</li></br>
     * </ul>
     * </p>
     */
    @SuppressWarnings("deprecation")
    protected void checkApplication() {
        // 兼容旧版本
        if (application == null) {
            String applicationName = ConfigUtils.getProperty("dubbo.application.name");
            if (StringUtils.isNotEmpty(applicationName)) {
                application = new ApplicationConfig();
            }
        }
        if (application == null) {
            throw new IllegalStateException("No such application config! Please add <dubbo:application name=\"...\" /> to your spring config.");
        }
        appendProperties(application);

        if (registries == null) {
            registries = application.getRegistries();
        }
        if (monitor == null) {
            monitor = application.getMonitor();
        }
        //属性变化下
        String wait = ConfigUtils.getProperty(Constants.SHUTDOWN_WAIT_KEY);
        if (StringUtils.isNotEmpty(wait)) {
            System.setProperty(Constants.SHUTDOWN_WAIT_KEY, wait.trim());
            return;
        }
        wait = ConfigUtils.getProperty(Constants.SHUTDOWN_WAIT_SECONDS_KEY);
        if (isEmpty(wait)) {
            return;
        }
        System.setProperty(Constants.SHUTDOWN_WAIT_SECONDS_KEY, wait.trim());
    }

    /**
     * 从注册配置中加载所有的URL
     * <ul>
     * <li>检查属性registries的配置，不存在会生成，{@link #checkRegistry()}</li><br/>
     * <li>对registries进行处理，对每一个register都尝试生成对应的URL生成规则如下
     * <ul>
     * <li>检验register的地址，没有配置默认是本机0.0.0.0</li><br/>
     * <li>尝试从java系统变量中获得环境变量为dubbo.registry.address的值，从而尝试作为地址，来覆盖默认处理</li><br/>
     * <li>对符合的有效的地址进一步处理，有效的概念是:地址存在，且不是{@link RegistryConfig#NO_AVAILABLE}</li><br/>
     * </ul>
     * </li>
     * <li>处理规则如下:
     * <ul>
     * <li>尝试从配置类application中获得键值对元信息</li><br/>
     * <li>尝试从配置类register中获得键值对元信息</li><br/>
     * <li>添加key:path;value:com.alibaba.dubbo.registry.RegistryService</li><br/>
     * <li>添加key:dubbo;value:2.0.0</li><br/>
     * <li>添加key:timestamp;value:${当前时间戳}</li><br/>
     * <li>添加key:pid;value:${当前时间戳}</li><br/>
     * <li>对元信息中不含建“protocol"的处理，如果扩展类remote存在，添加键值对("protocol:remote"),否则添加键值对("protocol:dubbo")</li>
     * <li>根据地址和元信息map生成URL</li>
     * <li>添加key:registry;value:${URL.protocol}</li>
     * <li>设置URL的protocol为registry,这样处理后，相当于进行了转移</li>
     * </ul></li>
     * </ul>
     *
     * @param provider
     * @return
     * @see com.alibaba.dubbo.registry.integration.RegistryProtocol#getRegistry(Invoker)
     */
    protected List<URL> loadRegistries(boolean provider) {
        //检查注册配置
        checkRegistry();
        List<URL> registryList = new ArrayList<URL>();
        for (RegistryConfig config : registries) {
            //再次确定远程注册中心的地址
            String address = System.getProperty("dubbo.registry.address");//获得系统配置地址
            if (isEmpty(address)) {
                address = config.getAddress();
            }
            if (isEmpty(address)) {
                address = ANYHOST_VALUE;//本机
            }
            if (NO_AVAILABLE.equalsIgnoreCase(address)) {
                continue;
            }
            //有效的注册中心地址即不是N/A
            Map<String, String> map = new HashMap<String, String>();
            //从应用中获得键值对
            appendParameters(map, application);
            //从应用中获得注册配置获得键值对
            appendParameters(map, config);
            map.put("path", RegistryService.class.getName());
            map.put("dubbo", getVersion());
            map.put(TIMESTAMP_KEY, String.valueOf(currentTimeMillis()));
            map.put(Constants.PID_KEY, String.valueOf(getPid()));

            for (URL url : UrlUtils.parseURLs(address, map)) {
                url = url.addParameter(REGISTRY_KEY, url.getProtocol());//url中的map加入registry，url.getProtocol()
                url = url.setProtocol(REGISTRY_PROTOCOL);//url中对protocol重新设置，为registry
                //上面这样设置的目的是为了用protocol为registry代表这个url代表了一个注册中心。同时url的参数信息（registry:原先的的protocol代表注册中心的特别类型，比如zookeeper，redis
                if ((provider && url.getParameter(REGISTER_KEY, true))) {//for服务提供者
                    registryList.add(url);
                }
                if (!provider && url.getParameter(SUBSCRIBE_KEY, true)) {//for服务消费者
                    registryList.add(url);
                }
            }
        }
        return registryList;
    }

    protected URL loadMonitor(URL registryURL) {
        if (monitor == null) {
            String monitorAddress = ConfigUtils.getProperty("dubbo.monitor.address");
            String monitorProtocol = ConfigUtils.getProperty("dubbo.monitor.protocol");
            if (monitorAddress != null && monitorAddress.length() > 0
                    || monitorProtocol != null && monitorProtocol.length() > 0) {
                monitor = new MonitorConfig();
            } else {
                return null;
            }
        }
        appendProperties(monitor);
        Map<String, String> map = new HashMap<String, String>();
        map.put(INTERFACE_KEY, MonitorService.class.getName());
        map.put("dubbo", getVersion());
        map.put(TIMESTAMP_KEY, String.valueOf(currentTimeMillis()));
        if (getPid() > 0) {
            map.put(Constants.PID_KEY, String.valueOf(getPid()));
        }
        appendParameters(map, monitor);
        String address = monitor.getAddress();
        String sysaddress = System.getProperty("dubbo.monitor.address");
        if (isNotEmpty(sysaddress)) {
            address = sysaddress;
        }
        if (ConfigUtils.isNotEmpty(address)) {
            if (!map.containsKey(Constants.PROTOCOL_KEY)) {
                if (ExtensionLoader.getExtensionLoader(MonitorFactory.class).hasExtension("logstat")) {
                    map.put(Constants.PROTOCOL_KEY, "logstat");
                } else {
                    map.put(Constants.PROTOCOL_KEY, "dubbo");
                }
            }
            return UrlUtils.parseURL(address, map);
        } else if (REGISTRY_PROTOCOL.equals(monitor.getProtocol()) && registryURL != null) {
            return registryURL.setProtocol("dubbo").addParameter(Constants.PROTOCOL_KEY, "registry")
                    .addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map));
        }
        return null;
    }

    /**
     * MethodConfig标签为<dubbo:service>或<dubbo:reference>的子标签，用于控制到方法级，
     *
     * @param interfaceClass 引用接口类
     * @param methods        接口类对应的方法配置类
     */
    protected void checkInterfaceAndMethods(Class<?> interfaceClass, List<MethodConfig> methods) {
        // 接口不能为空
        if (interfaceClass == null) {
            throw new IllegalStateException("interface not allow null!");
        }
        // 检查接口类型必需为接口
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("The interface class " + interfaceClass + " is not a interface!");
        }
        if (methods == null || methods.size() == 0) {
            return;
        }
        // 所有方法配置类中配置的方法信息，必须能映射到暴露接口的方法中，这种映射关系就是两者的名字是相等的。
        Set<String> methodName = new HashSet<String>();
        for (Method method : interfaceClass.getMethods()) {
            methodName.add(method.getName());
        }
        for (MethodConfig methodConfig : methods) {
            String methodConfigName = methodConfig.getName();
            if (!methodName.contains(methodConfigName)) {
                throw new IllegalStateException("<dubbo:method> name attribute is required and in interface's method name! Please check: <dubbo:service interface=\"" + interfaceClass.getName()
                        + "\" ... ><dubbo:method name=\"\" ... /></<dubbo:reference>; you method's name is: " + methodConfigName);
            }
        }
    }

    /**
     * <p>
     * 检验local、stub和mock设置的正确性<br/>
     * local不建议被使用和stub功能一致
     * 一旦配置，相关的桩类应该要在程序中能找到，
     * 桩类必须提供一个构造函数，完成对接口类的包装
     * <p>
     * mock类有不同的处理:mock 可以配置片段代码，直接进行返回，否则同local和stub
     * </p>
     *
     * @param interfaceClass 引用接口类
     */
    protected void checkStubAndMock(Class<?> interfaceClass) {
        //locol的检查
        if (ConfigUtils.isNotEmpty(local)) {
            Class<?> localClass = ConfigUtils.isDefault(local) ? ReflectUtils.forName(interfaceClass.getName() + "Local") : ReflectUtils.forName(local);
            if (!interfaceClass.isAssignableFrom(localClass)) {
                throw new IllegalStateException("The local implemention class " + localClass.getName() + " not implement interface " + interfaceClass.getName());
            }
            try {
                ReflectUtils.findConstructor(localClass, interfaceClass);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("No such constructor \"public " + localClass.getSimpleName() + "(" + interfaceClass.getName() + ")\" in local implemention class " + localClass.getName());
            }
        }
        //stub的检查
        if (ConfigUtils.isNotEmpty(stub)) {
            Class<?> localClass = ConfigUtils.isDefault(stub) ? ReflectUtils.forName(interfaceClass.getName() + "Stub") : ReflectUtils.forName(stub);
            if (!interfaceClass.isAssignableFrom(localClass)) {
                throw new IllegalStateException("The local implemention class " + localClass.getName() + " not implement interface " + interfaceClass.getName());
            }
            try {
                ReflectUtils.findConstructor(localClass, interfaceClass);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("No such constructor \"public " + localClass.getSimpleName() + "(" + interfaceClass.getName() + ")\" in local implemention class " + localClass.getName());
            }
        }
        //生成mock
        if (ConfigUtils.isNotEmpty(mock)) {
            if (mock.startsWith(Constants.RETURN_PREFIX)) { // 对应mock直接配置为代码字符串的，要求以return打头，我们将这字符串进行简单的解释
                String value = mock.substring(Constants.RETURN_PREFIX.length());
                try {
                    MockInvoker.parseMockValue(value);
                } catch (Exception e) {
                    throw new IllegalStateException("Illegal mock json value in <dubbo:service ... mock=\"" + mock + "\" />");
                }
            } else {
                //是类的情况，由一个控制字段来决定事件的mock的类名，mock为true和default的，使用系统推导，否则使用mock的默认值
                Class<?> mockClass = ConfigUtils.isDefault(mock) ? ReflectUtils.forName(interfaceClass.getName() + "Mock") : ReflectUtils.forName(mock);
                if (!interfaceClass.isAssignableFrom(mockClass)) {
                    throw new IllegalStateException("The mock implemention class " + mockClass.getName() + " not implement interface " + interfaceClass.getName());
                }
                try {
                    mockClass.getConstructor(new Class<?>[0]);
                } catch (NoSuchMethodException e) {
                    throw new IllegalStateException("No such empty constructor \"public " + mockClass.getSimpleName() + "()\" in mock implemention class " + mockClass.getName());
                }
            }
        }
    }

    /**
     * @return local
     * @deprecated Replace to <code>getStub()</code>
     */
    @Deprecated
    public String getLocal() {
        return local;
    }

    /**
     * @param local
     * @deprecated Replace to <code>setStub(String)</code>
     */
    @Deprecated
    public void setLocal(String local) {
        checkName("local", local);
        this.local = local;
    }

    /**
     * @param local
     * @deprecated Replace to <code>setStub(Boolean)</code>
     */
    @Deprecated
    public void setLocal(Boolean local) {
        if (local == null) {
            setLocal((String) null);
        } else {
            setLocal(String.valueOf(local));
        }
    }

    public String getStub() {
        return stub;
    }

    public void setStub(String stub) {
        checkName("stub", stub);
        this.stub = stub;
    }

    public void setStub(Boolean stub) {
        if (local == null) {
            setStub((String) null);
        } else {
            setStub(String.valueOf(stub));
        }
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        checkExtension(Cluster.class, "cluster", cluster);
        this.cluster = cluster;
    }

    public String getProxy() {
        return proxy;
    }

    public void setProxy(String proxy) {
        checkExtension(ProxyFactory.class, "proxy", proxy);
        this.proxy = proxy;
    }

    public Integer getConnections() {
        return connections;
    }

    public void setConnections(Integer connections) {
        this.connections = connections;
    }

    @Parameter(key = Constants.REFERENCE_FILTER_KEY, append = true)
    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        checkMultiExtension(Filter.class, "filter", filter);
        this.filter = filter;
    }

    @Parameter(key = Constants.INVOKER_LISTENER_KEY, append = true)
    public String getListener() {
        checkMultiExtension(InvokerListener.class, "listener", listener);
        return listener;
    }

    public void setListener(String listener) {
        this.listener = listener;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        checkNameHasSymbol("layer", layer);
        this.layer = layer;
    }

    public ApplicationConfig getApplication() {
        return application;
    }

    public void setApplication(ApplicationConfig application) {
        this.application = application;
    }

    public ModuleConfig getModule() {
        return module;
    }

    public void setModule(ModuleConfig module) {
        this.module = module;
    }

    public RegistryConfig getRegistry() {
        return registries == null || registries.size() == 0 ? null : registries.get(0);
    }

    public void setRegistry(RegistryConfig registry) {
        List<RegistryConfig> registries = new ArrayList<RegistryConfig>(1);
        registries.add(registry);
        this.registries = registries;
    }

    public List<RegistryConfig> getRegistries() {
        return registries;
    }

    @SuppressWarnings({"unchecked"})
    public void setRegistries(List<? extends RegistryConfig> registries) {
        this.registries = (List<RegistryConfig>) registries;
    }

    public MonitorConfig getMonitor() {
        return monitor;
    }

    public void setMonitor(MonitorConfig monitor) {
        this.monitor = monitor;
    }

    public void setMonitor(String monitor) {
        this.monitor = new MonitorConfig(monitor);
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        checkMultiName("owner", owner);
        this.owner = owner;
    }

    public void setCallbacks(Integer callbacks) {
        this.callbacks = callbacks;
    }

    public Integer getCallbacks() {
        return callbacks;
    }

    public String getOnconnect() {
        return onconnect;
    }

    public void setOnconnect(String onconnect) {
        this.onconnect = onconnect;
    }

    public String getOndisconnect() {
        return ondisconnect;
    }

    public void setOndisconnect(String ondisconnect) {
        this.ondisconnect = ondisconnect;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

}