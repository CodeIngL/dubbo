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
package com.alibaba.dubbo.common.extension;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.support.ActivateComparator;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.Holder;
import com.alibaba.dubbo.common.utils.StringUtils;

/**
 * Dubbo使用的扩展点获取<p>
 * <ul>
 * <li>自动注入关联扩展点。</li>
 * <li>自动Wrap上扩展点的Wrap类。</li>
 * <li>缺省获得的的扩展点是一个Adaptive Instance。</li>
 * </ul>
 *
 * @author william.liangf
 * @author ding.lid
 * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">JDK5.0的自动发现机制实现</a>
 * @see com.alibaba.dubbo.common.extension.SPI
 * @see com.alibaba.dubbo.common.extension.Adaptive
 * @see com.alibaba.dubbo.common.extension.Activate
 */
public class ExtensionLoader<T> {

    // ============类属性==================

    private static final Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);

    //services目录，用于寻找扩展的匹配类
    private static final String SERVICES_DIRECTORY = "META-INF/services/";

    //dubbo目录，用于寻找扩展的匹配类
    private static final String DUBBO_DIRECTORY = "META-INF/dubbo/";

    //dubbo内部目录，用于寻找扩展的匹配类
    private static final String DUBBO_INTERNAL_DIRECTORY = DUBBO_DIRECTORY + "internal/";

    //名字分割符
    private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");

    //全局，缓存了class（这个class特指interface），与ExtensionLoader（interface扩展加载类的实现）的映射
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<Class<?>, ExtensionLoader<?>>();

    //实例结构：class来自普通实现类型，见cachedClasses
    private static final ConcurrentMap<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<Class<?>, Object>();

    // ==============对象属性================

    //T的类型，interface类型
    private final Class<?> type;

    //T为ExtensionFactory类实例(应用单例)该项为空
    //others:AdaptiveExtensionFactory实例(单例)
    private final ExtensionFactory objectFactory;

//    三种情况介绍:
//    1. 带有@Adaptive注解的实现
//    2. T被构造函数作为包装的实现
//    3. 普通class.forName的实现

    //缓存:普通实现实例和名字的缓存,一个普通实现实例可以带有多个名字哦
    //ex:aa,bb=com.codeL.Demo;
    //则会缓存com.codeL.Demo:aa
    private final ConcurrentMap<Class<?>, String> cachedNames = new ConcurrentHashMap<Class<?>, String>();

    //缓存:名字和普通实现实例的缓存，这个名字总是配置的第一项。
    //ex:aa,bb=com.codeL.Demo;
    //则会缓存aa:com.codeL.Demo和bb:com.codeL.Demo
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<Map<String, Class<?>>>();

    //缓存:名字和普通实现实例的缓存，这个名字总是配置的第一项。
    //同时这个实现类，必须带有注解Activate
    private final Map<String, Activate> cachedActivates = new ConcurrentHashMap<String, Activate>();

    //适配类型的缓存，带有注解@Adaptive。
    private volatile Class<?> cachedAdaptiveClass = null;

    //缓存实例
    private final ConcurrentMap<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<String, Holder<Object>>();

    //名字:总是SPI注解名字的第一项
    //ex:@SPI(value="aa,bb")
    //则名字就是aa
    private String cachedDefaultName;

    //适配类型实例缓存，即带有注解@Adaptive的缓存，没有会被代码生成实现
    private final Holder<Object> cachedAdaptiveInstance = new Holder<Object>();

    //错误关于AdaptiveInstance
    private volatile Throwable createAdaptiveInstanceError;

    //T被装饰后的实例缓存，等价于静态代理，代理必须还有类型的构造函数。
    private Set<Class<?>> cachedWrapperClasses;

    //错误映射
    private Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<String, IllegalStateException>();


    private static <T> boolean withExtensionAnnotation(Class<T> type) {
        return type.isAnnotationPresent(SPI.class);
    }

    //对传递进来的interfac类type进行扩展实现，
    //这个实现可能是编码实现的存在文件中
    //亦可能是运行生成的

    /**
     * 根据type的接口，寻找tepe的实现，并返回实现类的扩展加载类。
     * <p>
     * <b>ex</b>: type equal to com.A, the type's impl is com.AImpl, the result will return ExtensionLoader<AImpl>.<br/>
     * <b>tip</b>: this will cache in EXTENSION_LOADERS with key:com.A  , value:ExtensionLoader<AImpl>.<br/>
     * </p>
     * <ul>
     * <li>type非空，并必须为接口类型，同时带有SPI注解</li>
     * </ul>
     *
     * @param type 接口类型
     * @param <T>  type实现类的扩展加载类型。
     * @return 特定的扩展加载类
     */
    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        //参数非空检查
        if (type == null)
            throw new IllegalArgumentException("Extension type == null");
        //参数类型检查，必须是interface
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type(" + type + ") is not interface!");
        }
        //interface必须带有spi注解
        if (!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException("Extension type(" + type +
                    ") is not extension, because WITHOUT @" + SPI.class.getSimpleName() + " Annotation!");
        }
        //尝试从全局EXTENSION_LOADERS实例缓存集合中获取，loader
        //没有则新建loader,并放置type和loader映射
        ExtensionLoader<T> loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        if (loader == null) {
            EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<T>(type));
            loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        }
        return loader;
    }

    //内部构造
    //对于objectFactory，如果type是ExtensionFactory，那么objectFactory是空的，否则，都是适配,总是ExtensionLoader<ExtensionFactory>的适配，这个是单例的

    /**
     * 扩展加载类构造函数<br/>
     * 对接口类型type，寻找其实现类，并完成实现类的扩展加载类初始化
     * <ul>
     * <li>完成扩展加载类的type属性的赋值,该属性区分了不同的扩展加载类</li><br/>
     * <li>对type接口为ExtensionFactory，则该扩展加载器类的{@link #objectFactory}属性为null</li><br/>
     * <li>对type接口为其他类，则该扩展加载器类的{@link #objectFactory}属性为{@link com.alibaba.dubbo.common.extension.factory.AdaptiveExtensionFactory}</li><br/>
     * </ul>
     *
     * @param type 接口类型
     * @see #getAdaptiveExtension
     */
    private ExtensionLoader(Class<?> type) {
        //赋值类型
        this.type = type;
        //使用ExtensionFactory包装
        if (type == ExtensionFactory.class) {
            objectFactory = null;
        } else {
            ExtensionLoader<ExtensionFactory> factory = ExtensionLoader.getExtensionLoader(ExtensionFactory.class);
            objectFactory = factory.getAdaptiveExtension();
        }
    }

    public String getExtensionName(T extensionInstance) {
        return getExtensionName(extensionInstance.getClass());
    }

    public String getExtensionName(Class<?> extensionClass) {
        return cachedNames.get(extensionClass);
    }

    /**
     * This is equivalent to <pre>
     *     getActivateExtension(url, key, null);
     * </pre>
     *
     * @param url url
     * @param key url parameter key which used to get extension point names
     * @return extension list which are activated.
     * @see #getActivateExtension(com.alibaba.dubbo.common.URL, String, String)
     */
    public List<T> getActivateExtension(URL url, String key) {
        return getActivateExtension(url, key, null);
    }

    /**
     * This is equivalent to <pre>
     *     getActivateExtension(url, values, null);
     * </pre>
     *
     * @param url    url
     * @param values extension point names
     * @return extension list which are activated
     * @see #getActivateExtension(com.alibaba.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String[] values) {
        return getActivateExtension(url, values, null);
    }

    /**
     * This is equivalent to <pre>
     *     getActivateExtension(url, url.getParameter(key).split(","), null);
     * </pre>
     *
     * @param url   url
     * @param key   url parameter key which used to get extension point names
     * @param group group
     * @return extension list which are activated.
     * @see #getActivateExtension(com.alibaba.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String key, String group) {
        String value = url.getParameter(key);
        //This is equivalent to getActivateExtension(url, url.getParameter(key).split(","), null);
        return getActivateExtension(url, value == null || value.length() == 0 ? null : Constants.COMMA_SPLIT_PATTERN.split(value), group);
    }

    /**
     * Get activate extensions.
     *
     * @param url    url
     * @param values extension point names
     * @param group  group
     * @return extension list which are activated
     * @see com.alibaba.dubbo.common.extension.Activate
     */
    public List<T> getActivateExtension(URL url, String[] values, String group) {
        List<T> exts = new ArrayList<T>();
        List<String> names = values == null ? new ArrayList<String>(0) : Arrays.asList(values);
        //name 不包含-default的值，才会操作
        if (!names.contains(Constants.REMOVE_VALUE_PREFIX + Constants.DEFAULT_KEY)) {
            getExtensionClasses();
            for (Map.Entry<String, Activate> entry : cachedActivates.entrySet()) {
                String name = entry.getKey();
                Activate activate = entry.getValue();
                if (isMatchGroup(group, activate.group())) {
                    T ext = getExtension(name);
                    if (!names.contains(name)
                            && !names.contains(Constants.REMOVE_VALUE_PREFIX + name)
                            && isActive(activate, url)) {
                        exts.add(ext);
                    }
                }
            }
            Collections.sort(exts, ActivateComparator.COMPARATOR);
        }
        //用户自己的
        List<T> usrs = new ArrayList<T>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (!name.startsWith(Constants.REMOVE_VALUE_PREFIX) && !names.contains(Constants.REMOVE_VALUE_PREFIX + name)) {
                if (Constants.DEFAULT_KEY.equals(name)) {
                    if (usrs.size() > 0) {
                        exts.addAll(0, usrs);
                        usrs.clear();
                    }
                } else {
                    T ext = getExtension(name);
                    usrs.add(ext);
                }
            }
        }
        if (usrs.size() > 0) {
            exts.addAll(usrs);
        }
        return exts;
    }


    /**
     * 是否在group列表中
     *
     * @param group  组名。provider or consumer
     * @param groups 列表
     * @return 是否在列表中
     */
    private boolean isMatchGroup(String group, String[] groups) {
        if (group == null || group.length() == 0) {
            return true;
        }
        if (groups != null && groups.length > 0) {
            for (String g : groups) {
                if (group.equals(g)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 是否激活
     *
     * @param activate 注解参数
     * @param url      元信息
     * @return 是否激活
     */
    private boolean isActive(Activate activate, URL url) {
        String[] keys = activate.value();
        if (keys == null || keys.length == 0) {
            return true;
        }
        //activate的key在元信息中，同时元信息对应的值不是空的，就激活
        for (String key : keys) {
            for (Map.Entry<String, String> entry : url.getParameters().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if ((k.equals(key) || k.endsWith("." + key))
                        && ConfigUtils.isNotEmpty(v)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 返回扩展点实例，如果没有指定的扩展点或是还没加载（即实例化）则返回<code>null</code>。注意：此方法不会触发扩展点的加载。
     * <p/>
     * 一般应该调用{@link #getExtension(String)}方法获得扩展，这个方法会触发扩展点加载。
     *
     * @see #getExtension(String)
     */
    @SuppressWarnings("unchecked")
    public T getLoadedExtension(String name) {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Extension name == null");
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<Object>());
            holder = cachedInstances.get(name);
        }
        return (T) holder.get();
    }

    /**
     * 返回已经加载的扩展点的名字。
     * <p/>
     * 一般应该调用{@link #getSupportedExtensions()}方法获得扩展，这个方法会返回所有的扩展点。
     *
     * @see #getSupportedExtensions()
     */
    public Set<String> getLoadedExtensions() {
        return Collections.unmodifiableSet(new TreeSet<String>(cachedInstances.keySet()));
    }

    /**
     * 返回指定名字的扩展。如果指定名字的扩展不存在，则抛异常 {@link IllegalStateException}.
     * 这个方法通常被$Adaptive中使用的
     *
     * @param name
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getExtension(String name) {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Extension name == null");
        if ("true".equals(name)) {
            return getDefaultExtension();
        }
        //尝试从缓存中获得
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<Object>());
            holder = cachedInstances.get(name);
        }
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    //创建实例，
                    instance = createExtension(name);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * 返回缺省的扩展，如果没有设置则返回<code>null</code>。
     */
    public T getDefaultExtension() {
        getExtensionClasses();
        if (null == cachedDefaultName || cachedDefaultName.length() == 0
                || "true".equals(cachedDefaultName)) {
            return null;
        }
        return getExtension(cachedDefaultName);
    }

    public boolean hasExtension(String name) {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Extension name == null");
        try {
            return getExtensionClass(name) != null;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * 返回缺省的扩展点名，如果没有设置缺省则返回<code>null</code>。
     */
    public String getDefaultExtensionName() {
        getExtensionClasses();
        return cachedDefaultName;
    }

    /**
     * 编程方式添加新扩展点。
     *
     * @param name  扩展点名
     * @param clazz 扩展点类
     * @throws IllegalStateException 要添加扩展点名已经存在。
     */
    public void addExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + "not implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + "can not be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " already existed(Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
        } else {
            if (cachedAdaptiveClass != null) {
                throw new IllegalStateException("Adaptive Extension already existed(Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
        }
    }

    /**
     * 编程方式添加替换已有扩展点。
     *
     * @param name  扩展点名
     * @param clazz 扩展点类
     * @throws IllegalStateException 要添加扩展点名已经存在。
     * @deprecated 不推荐应用使用，一般只在测试时可以使用
     */
    @Deprecated
    public void replaceExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + "not implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + "can not be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (!cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " not existed(Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
            cachedInstances.remove(name);
        } else {
            if (cachedAdaptiveClass == null) {
                throw new IllegalStateException("Adaptive Extension not existed(Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
            cachedAdaptiveInstance.set(null);
        }
    }

    /**
     * 对于扩展加载器类的具体泛型T，进行获得适配扩展类
     * 该部分代码，参考来自spring，xmlBeanFactory。
     * <ul>
     * <li>从缓存属性{@link #cachedAdaptiveInstance}中获得</li><br/>
     * <li>无则使用{@link #createAdaptiveExtension}新建获得适配扩展类，并完成对缓存属性{@link #cachedAdaptiveInstance}的设置</li><br/>
     * </ul>
     * 一般情况下不会有并发访问这个情况
     * cachedAdaptiveInstance这个属性只有在该函数调用后才会发生设置
     * 对应ExtensionLoader<T>实例这总是可选调用的，而这个实例总是被维护在EXTENSION_LOADERS中，是线程安全且单例存在的
     *
     * @return T的适配扩展类
     */
    @SuppressWarnings("unchecked")
    public T getAdaptiveExtension() {
        //尝试从cachedAdaptiveInstance属性中直接获取，没有则委托给createAdaptiveExtension创建interface的适配实例T
        Object instance = cachedAdaptiveInstance.get();
        if (instance == null) {
            if (createAdaptiveInstanceError == null) {
                synchronized (cachedAdaptiveInstance) {
                    instance = cachedAdaptiveInstance.get();
                    if (instance == null) {
                        try {
                            //创建适配实例
                            instance = createAdaptiveExtension();
                            //设置cachedAdaptiveInstance属性
                            cachedAdaptiveInstance.set(instance);
                        } catch (Throwable t) {
                            createAdaptiveInstanceError = t;
                            throw new IllegalStateException("fail to create adaptive instance: " + t.toString(), t);
                        }
                    }
                }
            } else {
                throw new IllegalStateException("fail to create adaptive instance: " + createAdaptiveInstanceError.toString(), createAdaptiveInstanceError);
            }
        }
        return (T) instance;
    }

    private IllegalStateException findException(String name) {
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (entry.getKey().toLowerCase().contains(name.toLowerCase())) {
                return entry.getValue();
            }
        }
        StringBuilder buf = new StringBuilder("No such extension " + type.getName() + " by name " + name);


        int i = 1;
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (i == 1) {
                buf.append(", possible causes: ");
            }

            buf.append("\r\n(");
            buf.append(i++);
            buf.append(") ");
            buf.append(entry.getKey());
            buf.append(":\r\n");
            buf.append(StringUtils.toString(entry.getValue()));
        }
        return new IllegalStateException(buf.toString());
    }

    /**
     * 注入实例
     *
     * @param instance
     * @return
     */
    private T injectExtension(T instance) {
        try {
            //不是Factory
            if (objectFactory != null) {
                //遍历所有的方法
                for (Method method : instance.getClass().getMethods()) {
                    //set方法，1个参数，公有
                    if (method.getName().startsWith("set")
                            && method.getParameterTypes().length == 1
                            && Modifier.isPublic(method.getModifiers())) {
                        //获得参数类型
                        Class<?> pt = method.getParameterTypes()[0];
                        try {
                            String property = method.getName().length() > 3 ? method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4) : "";
                            //尝试从Factory中获取
                            //这个factory总是AdaptiveExtensionFactory，
                            Object object = objectFactory.getExtension(pt, property);
                            if (object != null) {
                                method.invoke(instance, object);
                            }
                        } catch (Exception e) {
                            logger.error("fail to inject via method " + method.getName()
                                    + " of interface " + type.getName() + ": " + e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return instance;
    }


    /**
     * 新建扩展
     * 首先尝试根据名字在缓存中获得calss
     *
     * @param name
     * @return
     */
    @SuppressWarnings("unchecked")
    private T createExtension(String name) {
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null) {
            throw findException(name);
        }
        try {
            //直接新建
            T instance = (T) EXTENSION_INSTANCES.get(clazz);
            if (instance == null) {
                EXTENSION_INSTANCES.putIfAbsent(clazz, (T) clazz.newInstance());
                instance = (T) EXTENSION_INSTANCES.get(clazz);
            }

            //注入相关的实例
            injectExtension(instance);

            //层层包装
            Set<Class<?>> wrapperClasses = cachedWrapperClasses;
            if (wrapperClasses != null && wrapperClasses.size() > 0) {
                for (Class<?> wrapperClass : wrapperClasses) {
                    //注入包装实例，
                    //返回新实例
                    instance = injectExtension((T) wrapperClass.getConstructor(type).newInstance(instance));
                }
            }
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " +
                    type + ")  could not be instantiated: " + t.getMessage(), t);
        }
    }


    /**
     * 获得所有支持的扩展，排序后的
     *
     * @return
     */
    public Set<String> getSupportedExtensions() {
        Map<String, Class<?>> clazzes = getExtensionClasses();
        return Collections.unmodifiableSet(new TreeSet<String>(clazzes.keySet()));
    }

    /**
     * @param name
     * @return
     */
    private Class<?> getExtensionClass(String name) {
        if (type == null)
            throw new IllegalArgumentException("Extension type == null");
        if (name == null)
            throw new IllegalArgumentException("Extension name == null");
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null)
            throw new IllegalStateException("No such extension \"" + name + "\" for " + type.getName() + "!");
        return clazz;
    }

    /**
     * 获得扩展类
     * 这些类会被设置到属性cachedClasses中
     *
     * @return
     * @see #cachedClasses
     */
    private Map<String, Class<?>> getExtensionClasses() {
        //尝试从缓存中获取
        Map<String, Class<?>> classes = cachedClasses.get();
        //和之前一样的策略
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (classes == null) {
                    //尝试加载
                    classes = loadExtensionClasses();
                    //把map返回出来
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    /**
     * 加载扩展类
     * 从配置文件中加载
     * <ul>
     * <li>获得泛型T（type属性）对应接口的@SPI注解</li><br/>
     * <li>或的注解的value字段</li><br/>
     * <li>对于value值不为空串，则不能带有分隔符','，完成缓存属性cachedDefaultName的设置为value</li><br/>
     * <li>从配置文件{@link #DUBBO_INTERNAL_DIRECTORY}中查找，完成配置加载</li><br/>
     * <li>从配置文件{@link #DUBBO_DIRECTORY}中查找，完成配置加载</li><br/>
     * <li>从配置文件{@link #SERVICES_DIRECTORY}中查找，完成配置加载</li><br/>
     * </ul>
     *
     * @return
     * @see #loadFile(Map, String)
     */
    // 此方法已经getExtensionClasses方法調用过。
    private Map<String, Class<?>> loadExtensionClasses() {
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if (defaultAnnotation != null) {
            String value = defaultAnnotation.value();
            if ((value = value.trim()).length() > 0) {
                String[] names = NAME_SEPARATOR.split(value);
                if (names.length > 1) {
                    throw new IllegalStateException("more than 1 default extension name on extension " + type.getName()
                            + ": " + Arrays.toString(names));
                }
                if (names.length == 1) cachedDefaultName = names[0];
            }
        }
        //尝试加载，名字和类的关系，在配置文件中体现，比如a,b=com.codeL.A,则名字会存储，a:com.codeL.A和b:com.codeL.
        //但是实际缓存中保存使用的名字是A，这个可以通过com.codeL.A拿到，因为他们是双向存储
        Map<String, Class<?>> extensionClasses = new HashMap<String, Class<?>>();
        loadFile(extensionClasses, DUBBO_INTERNAL_DIRECTORY);
        loadFile(extensionClasses, DUBBO_DIRECTORY);
        loadFile(extensionClasses, SERVICES_DIRECTORY);
        return extensionClasses;
    }

    /**
     * 搜索jar包中内置文件
     * 完成适配类的设置cachedAdaptiveClass
     * 设置cachedWrapperClasses：对于能被包装的类，即配置文件中的对应的类，该类是T的实现，并具备参数为T的构造函数
     * 设置cachedActivates，key是name，value是类的Activates注解
     * 设置cachedNames，key是class，value是别名，是符合名字中的第一个
     *
     * @param extensionClasses，名字和普通扩展类的集合
     * @param dir                          配置文件存在的目录
     */
    private void loadFile(Map<String, Class<?>> extensionClasses, String dir) {
        String fileName = dir + type.getName();
        try {
            Enumeration<java.net.URL> urls;
            ClassLoader classLoader = findClassLoader();
            if (classLoader != null) {
                urls = classLoader.getResources(fileName);
            } else {
                urls = ClassLoader.getSystemResources(fileName);
            }
            if (urls != null) {
                while (urls.hasMoreElements()) {
                    java.net.URL url = urls.nextElement();
                    try {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "utf-8"));
                        try {
                            String line = null;
                            while ((line = reader.readLine()) != null) {
                                final int ci = line.indexOf('#');
                                if (ci >= 0) line = line.substring(0, ci);
                                line = line.trim();
                                if (line.length() > 0) {
                                    try {
                                        String name = null;
                                        int i = line.indexOf('=');
                                        if (i > 0) {
                                            name = line.substring(0, i).trim();
                                            line = line.substring(i + 1).trim();
                                        }
                                        if (line.length() > 0) {
                                            //放射实例化
                                            Class<?> clazz = Class.forName(line, true, classLoader);
                                            if (!type.isAssignableFrom(clazz)) {
                                                throw new IllegalStateException("Error when load extension class(interface: " +
                                                        type + ", class line: " + clazz.getName() + "), class "
                                                        + clazz.getName() + "is not subtype of interface.");
                                            }
                                            //是否含有Adaptive注解
                                            if (clazz.isAnnotationPresent(Adaptive.class)) {
                                                if (cachedAdaptiveClass == null) {
                                                    cachedAdaptiveClass = clazz;
                                                } else if (!cachedAdaptiveClass.equals(clazz)) {
                                                    //找到多个不合法
                                                    throw new IllegalStateException("More than 1 adaptive class found: "
                                                            + cachedAdaptiveClass.getClass().getName()
                                                            + ", " + clazz.getClass().getName());
                                                }
                                            } else {
                                                try {
                                                    //反射实例含有type参数的构造函数
                                                    clazz.getConstructor(type);
                                                    Set<Class<?>> wrappers = cachedWrapperClasses;
                                                    if (wrappers == null) {
                                                        cachedWrapperClasses = new ConcurrentHashSet<Class<?>>();
                                                        wrappers = cachedWrapperClasses;
                                                    }
                                                    //适配加入
                                                    wrappers.add(clazz);
                                                } catch (NoSuchMethodException e) {
                                                    //没有上述构造函数，不能适配类
                                                    clazz.getConstructor();
                                                    //忘记写名字。tip：这一步不会发生的，配置的时候我们会配置的
                                                    if (name == null || name.length() == 0) {
                                                        name = findAnnotationName(clazz);
                                                        if (name == null || name.length() == 0) {
                                                            if (clazz.getSimpleName().length() > type.getSimpleName().length()
                                                                    && clazz.getSimpleName().endsWith(type.getSimpleName())) {
                                                                name = clazz.getSimpleName().substring(0, clazz.getSimpleName().length() - type.getSimpleName().length()).toLowerCase();
                                                            } else {
                                                                throw new IllegalStateException("No such extension name for the class " + clazz.getName() + " in the config " + url);
                                                            }
                                                        }
                                                    }
                                                    //拆分名字
                                                    String[] names = NAME_SEPARATOR.split(name);
                                                    if (names != null && names.length > 0) {
                                                        //获取类上的激活注解
                                                        Activate activate = clazz.getAnnotation(Activate.class);
                                                        if (activate != null) {
                                                            //放入激活缓存,第一个名字和Activate的关系
                                                            cachedActivates.put(names[0], activate);
                                                        }
                                                        //放入名字缓存，第一个名字和class的关系
                                                        if (!cachedNames.containsKey(clazz)) {
                                                            cachedNames.put(clazz, names[0]);
                                                        }
                                                        //遍历名字，
                                                        for (String n : names) {
                                                            //反向放置名字和类的关系，这个要返回出去的，map是带进来的e
                                                            Class<?> c = extensionClasses.get(n);
                                                            if (c == null) {
                                                                extensionClasses.put(n, clazz);
                                                            } else if (c != clazz) {
                                                                throw new IllegalStateException("Duplicate extension " + type.getName() + " name " + n + " on " + c.getName() + " and " + clazz.getName());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Throwable t) {
                                        IllegalStateException e = new IllegalStateException("Failed to load extension class(interface: " + type + ", class line: " + line + ") in " + url + ", cause: " + t.getMessage(), t);
                                        exceptions.put(line, e);
                                    }
                                }
                            } // end of while read lines
                        } finally {
                            reader.close();
                        }
                    } catch (Throwable t) {
                        logger.error("Exception when load extension class(interface: " +
                                type + ", class file: " + url + ") in " + url, t);
                    }
                } // end of while urls
            }
        } catch (Throwable t) {
            logger.error("Exception when load extension class(interface: " +
                    type + ", description file: " + fileName + ").", t);
        }
    }

    /**
     * 找名字,如果class带有注解，且注解的value存在，不为空串和空，名字value值
     * 注解不存在，就是简单名字。
     * ex ：class equal to XxxInvoker and type equal to Invoker
     * result：xxx
     *
     * @param clazz
     * @return
     */
    @SuppressWarnings("deprecation")
    private String findAnnotationName(Class<?> clazz) {
        com.alibaba.dubbo.common.Extension extension = clazz.getAnnotation(com.alibaba.dubbo.common.Extension.class);
        if (extension == null) {
            String name = clazz.getSimpleName();
            if (name.endsWith(type.getSimpleName())) {
                name = name.substring(0, name.length() - type.getSimpleName().length());
            }
            return name.toLowerCase();
        }
        return extension.value();
    }


    /**
     * 对于扩展加载器类的具体泛型T，进行获得适配扩展类，并完成依赖注入
     *
     * @return 适配扩展类
     * @see #getAdaptiveExtensionClass()
     * @see #injectExtension(Object)
     */
    @SuppressWarnings("unchecked")
    private T createAdaptiveExtension() {
        try {
            //获得封装扩展的实例，并注入
            return injectExtension((T) getAdaptiveExtensionClass().newInstance());
        } catch (Exception e) {
            throw new IllegalStateException("Can not create adaptive extenstion " + type + ", cause: " + e.getMessage(), e);
        }
    }


    /**
     * 获得泛型T的扩展适配类
     * 这个类就是属性cachedAdaptiveClass,
     * <ul>
     * <li>使用{@link #getExtensionClasses()}获得所用扩展，包括普通以及特殊的扩展类的加载</li><br/>
     * <li>从缓存属性cachedAdaptiveClass，之间获得，正常情况下，{@link #getExtensionClasses()}会完成对cachedAdaptiveClass的设置</li><br/>
     * <li>缓存属性不存在，使用{@link #createAdaptiveExtensionClass()}生成</li><br/>
     * </ul>
     * 首先尝试获得扩展类，getExtensionClasses。里面包括对cachedAdaptiveClass的设置
     * 如果都没有。则直接通过运行编译器生成。
     *
     * @return 获得泛型T的扩展适配类。
     * @see #createAdaptiveExtensionClass()
     */
    private Class<?> getAdaptiveExtensionClass() {
        getExtensionClasses();
        //有适配的
        if (cachedAdaptiveClass != null) {
            return cachedAdaptiveClass;
        }
        //没有则新建
        return cachedAdaptiveClass = createAdaptiveExtensionClass();
    }


    /**
     * 没有@Adaptive的适配，使用编译器直接生成类
     *
     * @return 适配扩展类
     */
    private Class<?> createAdaptiveExtensionClass() {
        String code = createAdaptiveExtensionClassCode();
        ClassLoader classLoader = findClassLoader();
        com.alibaba.dubbo.common.compiler.Compiler compiler = ExtensionLoader.getExtensionLoader(com.alibaba.dubbo.common.compiler.Compiler.class).getAdaptiveExtension();
        //编译加载
        return compiler.compile(code, classLoader);
    }

    /**
     * 为对于在配置文件中找不到相应的扩展配置类（@Adpative注解）泛型T生成编译代码，
     * 方便运行编译加入。
     *
     * @return
     */
    private String createAdaptiveExtensionClassCode() {
        StringBuilder codeBuidler = new StringBuilder();
        Method[] methods = type.getMethods();
        boolean hasAdaptiveAnnotation = false;
        for (Method m : methods) {
            if (m.isAnnotationPresent(Adaptive.class)) {
                hasAdaptiveAnnotation = true;
                break;
            }
        }
        // 完全没有Adaptive方法，则不需要生成Adaptive类,抛出异常
        if (!hasAdaptiveAnnotation)
            throw new IllegalStateException("No adaptive method on extension " + type.getName() + ", refuse to create the adaptive class!");
        //导包
        codeBuidler.append("package " + type.getPackage().getName() + ";");
        codeBuidler.append("\nimport " + ExtensionLoader.class.getName() + ";");
        codeBuidler.append("\npublic class " + type.getSimpleName() + "$Adpative" + " implements " + type.getCanonicalName() + " {");

        //遍历方法
        //方法上没有标注@Adaptive
        for (Method method : methods) {
            Class<?> rt = method.getReturnType();
            Class<?>[] pts = method.getParameterTypes();
            Class<?>[] ets = method.getExceptionTypes();

            Adaptive adaptiveAnnotation = method.getAnnotation(Adaptive.class);
            StringBuilder code = new StringBuilder(512);
            //对应没有注解的认出异常
            if (adaptiveAnnotation == null) {
                code.append("throw new UnsupportedOperationException(\"method ")
                        .append(method.toString()).append(" of interface ")
                        .append(type.getName()).append(" is not adaptive method!\");");
            }
            //含有注解
            else {
                //如果方法含有URL参数，确定URL的位置
                int urlTypeIndex = -1;
                for (int i = 0; i < pts.length; ++i) {
                    if (pts[i].equals(URL.class)) {
                        urlTypeIndex = i;
                        break;
                    }
                }

                // 有类型为URL的参数
                if (urlTypeIndex != -1) {
                    // Null Point check
                    //对url参数进行校验
                    String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"url == null\");",
                            urlTypeIndex);
                    code.append(s);

                    //定义变量URL url =url对应的参数argx
                    s = String.format("\n%s url = arg%d;", URL.class.getName(), urlTypeIndex);
                    code.append(s);
                }
                // 参数没有URL类型，寻找参数实例中是否有URL的类型的方法
                else {
                    String attribMethod = null;

                    // 找到参数的URL属性
                    LBL_PTS:
                    for (int i = 0; i < pts.length; ++i) {
                        Method[] ms = pts[i].getMethods();
                        for (Method m : ms) {
                            String name = m.getName();
                            if ((name.startsWith("get") || name.length() > 3)
                                    && Modifier.isPublic(m.getModifiers())
                                    && !Modifier.isStatic(m.getModifiers())
                                    && m.getParameterTypes().length == 0
                                    && m.getReturnType() == URL.class) {
                                urlTypeIndex = i;
                                attribMethod = name;
                                break LBL_PTS;
                            }
                        }
                    }
                    //参数类里面属性依旧找不到
                    if (attribMethod == null) {
                        throw new IllegalStateException("fail to create adative class for interface " + type.getName()
                                + ": not found url parameter or url attribute in parameters of method " + method.getName());
                    }

                    //某个参数含有URL类型的方法
                    // Null point check
                    //先检验参数不为空
                    String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"%s argument == null\");",
                            urlTypeIndex, pts[urlTypeIndex].getName());
                    code.append(s);
                    //校验参数的url方法得到的url不为空
                    s = String.format("\nif (arg%d.%s() == null) throw new IllegalArgumentException(\"%s argument %s() == null\");",
                            urlTypeIndex, attribMethod, pts[urlTypeIndex].getName(), attribMethod);
                    code.append(s);

                    //得到url
                    s = String.format("%s url = arg%d.%s();", URL.class.getName(), urlTypeIndex, attribMethod);
                    code.append(s);
                }

                String[] value = adaptiveAnnotation.value();
                // 没有设置Key，则使用“扩展点接口名的点分隔 作为Key
                if (value.length == 0) {
                    //if charArrary equal to simpleName the result is simple.name
                    char[] charArray = type.getSimpleName().toCharArray();
                    StringBuilder sb = new StringBuilder(128);
                    for (int i = 0; i < charArray.length; i++) {
                        if (Character.isUpperCase(charArray[i])) {
                            if (i != 0) {
                                sb.append(".");
                            }
                            sb.append(Character.toLowerCase(charArray[i]));
                        } else {
                            sb.append(charArray[i]);
                        }
                    }
                    value = new String[]{sb.toString()};
                }

                //寻找参数类型是com.alibaba.dubbo.rpc.Invocation
                boolean hasInvocation = false;
                for (int i = 0; i < pts.length; ++i) {
                    if (pts[i].getName().equals("com.alibaba.dubbo.rpc.Invocation")) {
                        // Null Point check
                        //校验Invocation
                        String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"invocation == null\");", i);
                        code.append(s);
                        //获得方法名
                        s = String.format("\nString methodName = arg%d.getMethodName();", i);
                        code.append(s);
                        //设置标记含有Invocation
                        hasInvocation = true;
                        break;
                    }
                }
                //@SPI的value值
                //参数含有com.alibaba.dubbo.rpc.Invocation
                //URL url.getMethodParameter(methodName,value[i],defaultExtName)
                //先在URL上找key1的Value作为要Adapt成的Extension名；
                //key1没有Value，则使用key2的Value作为要Adapt成的Extension名。
                //key2没有Value，使用缺省的扩展。
                //如果没有设定缺省扩展，则方法调用会抛出IllegalStateException。
                String defaultExtName = cachedDefaultName;
                String getNameCode = null;
                for (int i = value.length - 1; i >= 0; --i) {
                    if (i == value.length - 1) {
                        //有默认参数名
                        if (null != defaultExtName) {
                            if (!"protocol".equals(value[i]))
                                if (hasInvocation)
                                    getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                                else
                                    getNameCode = String.format("url.getParameter(\"%s\", \"%s\")", value[i], defaultExtName);
                            else
                                getNameCode = String.format("( url.getProtocol() == null ? \"%s\" : url.getProtocol() )", defaultExtName);
                        } else {
                            if (!"protocol".equals(value[i]))
                                if (hasInvocation)
                                    getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                                else
                                    getNameCode = String.format("url.getParameter(\"%s\")", value[i]);
                            else
                                getNameCode = "url.getProtocol()";
                        }
                    } else {
                        if (!"protocol".equals(value[i]))
                            if (hasInvocation)
                                getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                            else
                                getNameCode = String.format("url.getParameter(\"%s\", %s)", value[i], getNameCode);
                        else
                            getNameCode = String.format("url.getProtocol() == null ? (%s) : url.getProtocol()", getNameCode);
                    }
                }
                code.append("\nString extName = ").append(getNameCode).append(";");
                // check extName == null?
                String s = String.format("\nif(extName == null) " +
                                "throw new IllegalStateException(\"Fail to get extension(%s) name from url(\" + url.toString() + \") use keys(%s)\");",
                        type.getName(), Arrays.toString(value));
                code.append(s);

                s = String.format("\n%s extension = (%<s)%s.getExtensionLoader(%s.class).getExtension(extName);",
                        type.getName(), ExtensionLoader.class.getSimpleName(), type.getName());
                code.append(s);

                // return statement
                if (!rt.equals(void.class)) {
                    code.append("\nreturn ");
                }

                s = String.format("extension.%s(", method.getName());
                code.append(s);
                for (int i = 0; i < pts.length; i++) {
                    if (i != 0)
                        code.append(", ");
                    code.append("arg").append(i);
                }
                code.append(");");
            }

            /**
             * 将上面生成的code追加进来
             * public 返回类型 方法名([参数类型1 arg0，参数类型2 args1...]) [throw 异常类型1，异常类型2...]{
             *      code.toString()
             * }
             */
            codeBuidler.append("\npublic " + rt.getCanonicalName() + " " + method.getName() + "(");
            for (int i = 0; i < pts.length; i++) {
                if (i > 0) {
                    codeBuidler.append(", ");
                }
                codeBuidler.append(pts[i].getCanonicalName());
                codeBuidler.append(" ");
                codeBuidler.append("arg" + i);
            }
            codeBuidler.append(")");
            if (ets.length > 0) {
                codeBuidler.append(" throws ");
                for (int i = 0; i < ets.length; i++) {
                    if (i > 0) {
                        codeBuidler.append(", ");
                    }
                    codeBuidler.append(ets[i].getCanonicalName());
                }
            }
            codeBuidler.append(" {");
            codeBuidler.append(code.toString());
            codeBuidler.append("\n}");
        }
        codeBuidler.append("\n}");
        if (logger.isDebugEnabled()) {
            logger.debug(codeBuidler.toString());
        }
        return codeBuidler.toString();
    }


    private static ClassLoader findClassLoader() {
        return ExtensionLoader.class.getClassLoader();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

}