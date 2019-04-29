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
package com.alibaba.dubbo.config.spring.schema;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.ArgumentConfig;
import com.alibaba.dubbo.config.ConsumerConfig;
import com.alibaba.dubbo.config.MethodConfig;
import com.alibaba.dubbo.config.MonitorConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ProviderConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.spring.ReferenceBean;
import com.alibaba.dubbo.config.spring.ServiceBean;
import com.alibaba.dubbo.rpc.Protocol;

import static java.lang.reflect.Modifier.isPublic;

/**
 * AbstractBeanDefinitionParser
 *
 * @author william.liangf
 * @export
 */
//模块自定义解析,所有的模块都是这个解析。真tm棒。
//为了区分解析的是什么，还注册相关类，来代表解析某个定义
public class DubboBeanDefinitionParser implements BeanDefinitionParser {

    private static final Logger logger = LoggerFactory.getLogger(DubboBeanDefinitionParser.class);
//    解析的类型
//    ApplicationConfig,
//    ModuleConfig,
//    RegistryConfig,
//    MonitorConfig,
//    ProviderConfig,
//    ConsumerConfig,
//    ProtocolConfig,
//    ServiceBean(ServiceConfig),
//    ReferenceBean(ReferenceConfig),
//    AnnotationBean(AbstractConfig特殊)
    private final Class<?> beanClass;

    //id是否必须，true的情况下，id没有，会生成id
    //false则忽略掉id
    private final boolean required;


    public DubboBeanDefinitionParser(Class<?> beanClass, boolean required) {
        this.beanClass = beanClass;
        this.required = required;
    }


    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {
        return parse(element, parserContext, beanClass, required);
    }

    //自定义解析
    @SuppressWarnings("unchecked")
    private static BeanDefinition parse(Element element, ParserContext parserContext, Class<?> beanClass, boolean required) {
        RootBeanDefinition beanDefinition = new RootBeanDefinition();
        beanDefinition.setBeanClass(beanClass);
        beanDefinition.setLazyInit(false);
        String id = element.getAttribute("id");
        //检测id属性
        //id没有就生成嘛。spring中id对于一个bean是必须有的属性
        if ((id == null || id.length() == 0) && required) {
            //获得name属性:能有name属性只有：application，method，module模块。
            String generatedBeanName = element.getAttribute("name");
            //没有配置name属性，几个特点的类不需要配置name属性
            if (generatedBeanName == null || generatedBeanName.length() == 0) {
                //beanClass是ProtocolConfig的缺省名字是dubbo
                if (ProtocolConfig.class.equals(beanClass)) {
                    generatedBeanName = "dubbo";
                } else {
                    //beanName是interface属性:典型暴露服务，和引用服务
                    //<dubbo:service interface="">
                    //<dubbo:reference interface=""/>
                    generatedBeanName = element.getAttribute("interface");
                }
            }
            //其他配置类
            if (generatedBeanName == null || generatedBeanName.length() == 0) {
                //直接上配置XXXConfig类名。
                generatedBeanName = beanClass.getName();
            }
            //一定存在的名字。id不存在:如果是Protocol类name是dubbo，如果是ServiceBean类或者ReferenceBean类name是interface的类名，其他就是xxxConfig的类名,id会追加序列区分bean。
            //tip：对于xml，这些name肯定是不存在的。配置是不支持的。
            //why?counter=2 why not counter=1?
            //2017/5/17:忽略第一个比如id=aa，实际上是aa1，我们忽略1，所以counter应该从2开始
            id = generatedBeanName;
            int counter = 2;
            //为了防止冲突。后面加序列来创建不同id
            while (parserContext.getRegistry().containsBeanDefinition(id)) {
                id = generatedBeanName + (counter++);
            }
        }

        //检查id了
        if (id != null && id.length() > 0) {
            //牛逼，抄的，检验重复ID，spring中不允许重复的id
            if (parserContext.getRegistry().containsBeanDefinition(id)) {
                throw new IllegalStateException("Duplicate spring bean id " + id);
            }
            //现在注册表中，注册了先
            parserContext.getRegistry().registerBeanDefinition(id, beanDefinition);
            //坑比，设置id属性，这个id在AbstractConfig中，剩余都是静态方法
            //AbstractConfig是所有配置类的基类
            beanDefinition.getPropertyValues().addPropertyValue("id", id);
        }

        //--------------------------------------------
        //粗略处理各种配置类
        //--------------------------------------------

        //处理ProtocolConfig(粗糙处理，毕竟还没处理)
        if (ProtocolConfig.class.equals(beanClass)) {
            //轮了一把beanDefine注册表，注入有依赖该类对象的bean定义，spring操作：
            for (String name : parserContext.getRegistry().getBeanDefinitionNames()) {
                BeanDefinition definition = parserContext.getRegistry().getBeanDefinition(name);
                //为了找某个beanDefine有protocol属性的
                PropertyValue property = definition.getPropertyValues().getPropertyValue("protocol");
                if (property != null) {
                    Object value = property.getValue();
                    if (value instanceof ProtocolConfig && id.equals(((ProtocolConfig) value).getName())) {
                        //看一下这个bean的protocol属性是不是ProtocolConfig这个如果是，当前id和这个名字相同。进行设置，
                        //就是注入依赖
                        definition.getPropertyValues().addPropertyValue("protocol", new RuntimeBeanReference(id));
                    }
                }
            }
        }

        //处理ServiceBean(粗糙处理，毕竟还没处理)，不使用interface和ref的处理而是使用interface和class的组合，那么嵌套的节点就是对class相关属性的注入
        else if (ServiceBean.class.equals(beanClass)) {
            //处理serviceBean这个定义，服务实现类
            String className = element.getAttribute("class");
            //存在class这个属性，这个属性是可选的。
            if (className != null && className.length() > 0) {
                //新建一个bean定义
                RootBeanDefinition classDefinition = new RootBeanDefinition();
                classDefinition.setBeanClass(ReflectUtils.forName(className));
                classDefinition.setLazyInit(false);
                //因此一定要解析
                parseProperties(element.getChildNodes(), classDefinition);
                //为serverBean添加一个额外属性 ref，指向这个相当于内部bean(classDefinition)。
                //so，为了注入spring环境中，这里使用了BeanDefinitionHolder中。tip：这个bean是没有name属性的
                //id是interface这个属性
                beanDefinition.getPropertyValues().addPropertyValue("ref", new BeanDefinitionHolder(classDefinition, id + "Impl"));
            }
        }

        //处理ProviderConfig(粗糙处理，毕竟还没处理)
        else if (ProviderConfig.class.equals(beanClass)) {
            //处理ProviderConfig定义，声明是服务提供者，可选配置
            parseNested(element, parserContext, ServiceBean.class, true, "service", "provider", id, beanDefinition);
        }
        //处理ConsumerConfig(粗糙处理，毕竟还没处理)
        else if (ConsumerConfig.class.equals(beanClass)) {
            //处理ConsumerConfig定义，声明是服务消费者
            parseNested(element, parserContext, ReferenceBean.class, false, "reference", "consumer", id, beanDefinition);
        }
        //细致处理中
        Set<String> props = new HashSet<String>();
        ManagedMap parameters = null;
        //变量
        for (Method setter : beanClass.getMethods()) {
            String name = setter.getName();
            //set方法,公有，参数长度1
            if (name.length() > 3 && name.startsWith("set")
                    && isPublic(setter.getModifiers())
                    && setter.getParameterTypes().length == 1) {
                //参数类型
                Class<?> type = setter.getParameterTypes()[0];
                String property = StringUtils.camelToSplitName(name.substring(3, 4).toLowerCase() + name.substring(4), "-");
                props.add(property);
                Method getter = null;
                //get方法。或者bool的is方法
                try {
                    getter = beanClass.getMethod("get" + name.substring(3), new Class<?>[0]);
                } catch (NoSuchMethodException e) {
                    try {
                        getter = beanClass.getMethod("is" + name.substring(3), new Class<?>[0]);
                    } catch (NoSuchMethodException e2) {
                    }
                }
                //没有get方法，get私有，get返回类型不和set一致，
                //跳过
                if (getter == null
                        || !isPublic(getter.getModifiers())
                        || !type.equals(getter.getReturnType())) {
                    continue;
                }
                //复杂属性处理
//                // 自定义参数,存在AbstractMethodConfig中
//                protected Map<String, String> parameters;
                if ("parameters".equals(property)) {
                    parameters = parseParameters(element.getChildNodes(), beanDefinition);
                }
//                // 方法配置，存在ServiceConfig中
//                private List<MethodConfig>  methods;
                else if ("methods".equals(property)) {
                    parseMethods(id, element.getChildNodes(), beanDefinition, parserContext);
                }
                // 存在MethodConfig中
//                private List<ArgumentConfig> arguments;
                else if ("arguments".equals(property)) {
                    parseArguments(id, element.getChildNodes(), beanDefinition, parserContext);
                } else {
                    //获得值
                    String value = element.getAttribute(property);
                    if (value != null) {
                        value = value.trim();
                        if (value.length() > 0) {
                            //属性是registry，值是N/A
                            if ("registry".equals(property) && RegistryConfig.NO_AVAILABLE.equalsIgnoreCase(value)) {
                                //系统默认策略
                                RegistryConfig registryConfig = new RegistryConfig();
                                registryConfig.setAddress(RegistryConfig.NO_AVAILABLE);
                                beanDefinition.getPropertyValues().addPropertyValue(property, registryConfig);
                            }
                            //属性是registry，值是多个
                            else if ("registry".equals(property) && value.indexOf(',') != -1) {
                                //解析多个引用
                                parseMultiRef("registries", value, beanDefinition, parserContext);
                            }
                            //同上
                            else if ("provider".equals(property) && value.indexOf(',') != -1) {
                                parseMultiRef("providers", value, beanDefinition, parserContext);
                            }
                            //同上
                            else if ("protocol".equals(property) && value.indexOf(',') != -1) {
                                parseMultiRef("protocols", value, beanDefinition, parserContext);
                            }
                            //其他属性
                            else {
                                Object reference;
                                if (isPrimitive(type)) {
                                    if ("async".equals(property) && "false".equals(value)
                                            || "timeout".equals(property) && "0".equals(value)
                                            || "delay".equals(property) && "0".equals(value)
                                            || "version".equals(property) && "0.0.0".equals(value)
                                            || "stat".equals(property) && "-1".equals(value)
                                            || "reliable".equals(property) && "false".equals(value)) {
                                        // 兼容旧版本xsd中的default值
                                        value = null;
                                    }
                                    reference = value;
                                } else if ("protocol".equals(property)
                                        && ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(value)
                                        && (!parserContext.getRegistry().containsBeanDefinition(value)
                                        || !ProtocolConfig.class.getName().equals(parserContext.getRegistry().getBeanDefinition(value).getBeanClassName()))) {
                                    if ("dubbo:provider".equals(element.getTagName())) {
                                        logger.warn("Recommended replace <dubbo:provider protocol=\"" + value + "\" ... /> to <dubbo:protocol name=\"" + value + "\" ... />");
                                    }
                                    // 兼容旧版本配置
                                    ProtocolConfig protocol = new ProtocolConfig();
                                    protocol.setName(value);
                                    reference = protocol;
                                } else if ("monitor".equals(property)
                                        && (!parserContext.getRegistry().containsBeanDefinition(value)
                                        || !MonitorConfig.class.getName().equals(parserContext.getRegistry().getBeanDefinition(value).getBeanClassName()))) {
                                    // 兼容旧版本配置
                                    reference = convertMonitor(value);
                                } else if ("onreturn".equals(property)) {
                                    int index = value.lastIndexOf(".");
                                    String returnRef = value.substring(0, index);
                                    String returnMethod = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(returnRef);
                                    beanDefinition.getPropertyValues().addPropertyValue("onreturnMethod", returnMethod);
                                } else if ("onthrow".equals(property)) {
                                    int index = value.lastIndexOf(".");
                                    String throwRef = value.substring(0, index);
                                    String throwMethod = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(throwRef);
                                    beanDefinition.getPropertyValues().addPropertyValue("onthrowMethod", throwMethod);
                                } else {
                                    if ("ref".equals(property) && parserContext.getRegistry().containsBeanDefinition(value)) {
                                        BeanDefinition refBean = parserContext.getRegistry().getBeanDefinition(value);
                                        if (!refBean.isSingleton()) {
                                            throw new IllegalStateException("The exported service ref " + value + " must be singleton! Please set the " + value + " bean scope to singleton, eg: <bean id=\"" + value + "\" scope=\"singleton\" ...>");
                                        }
                                    }
                                    reference = new RuntimeBeanReference(value);
                                }
                                beanDefinition.getPropertyValues().addPropertyValue(property, reference);
                            }
                        }
                    }
                }
            }
        }
        //字符串处理
        NamedNodeMap attributes = element.getAttributes();
        int len = attributes.getLength();
        for (int i = 0; i < len; i++) {
            Node node = attributes.item(i);
            String name = node.getLocalName();
            if (!props.contains(name)) {
                if (parameters == null) {
                    parameters = new ManagedMap();
                }
                String value = node.getNodeValue();
                parameters.put(name, new TypedStringValue(value, String.class));
            }
        }
        if (parameters != null) {
            beanDefinition.getPropertyValues().addPropertyValue("parameters", parameters);
        }
        return beanDefinition;
    }

    private static final Pattern GROUP_AND_VERION = Pattern.compile("^[\\-.0-9_a-zA-Z]+(\\:[\\-.0-9_a-zA-Z]+)?$");

    protected static MonitorConfig convertMonitor(String monitor) {
        if (monitor == null || monitor.length() == 0) {
            return null;
        }
        if (GROUP_AND_VERION.matcher(monitor).matches()) {
            String group;
            String version;
            int i = monitor.indexOf(':');
            if (i > 0) {
                group = monitor.substring(0, i);
                version = monitor.substring(i + 1);
            } else {
                group = monitor;
                version = null;
            }
            MonitorConfig monitorConfig = new MonitorConfig();
            monitorConfig.setGroup(group);
            monitorConfig.setVersion(version);
            return monitorConfig;
        }
        return null;
    }

    private static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || cls == Boolean.class || cls == Byte.class
                || cls == Character.class || cls == Short.class || cls == Integer.class
                || cls == Long.class || cls == Float.class || cls == Double.class
                || cls == String.class || cls == Date.class || cls == Class.class;
    }

    /**
     * 简单的处理多个引用
     * @param property
     * @param value
     * @param beanDefinition
     * @param parserContext
     */
    @SuppressWarnings("unchecked")
    private static void parseMultiRef(String property, String value, RootBeanDefinition beanDefinition,
                                      ParserContext parserContext) {
        //value就是ref
        String[] values = value.split("\\s*[,]+\\s*");
        ManagedList list = null;
        for (int i = 0; i < values.length; i++) {
            String v = values[i];
            if (v != null && v.length() > 0) {
                if (list == null) {
                    list = new ManagedList();
                }
                //简单的生成
                list.add(new RuntimeBeanReference(v));
            }
        }
        beanDefinition.getPropertyValues().addPropertyValue(property, list);
    }

    /**
     * 嵌套解析
     * @param element
     * @param parserContext
     * @param beanClass
     * @param required
     * @param tag
     * @param property
     * @param ref
     * @param beanDefinition
     */
//        parseNested(element, parserContext, ServiceBean.class, true, "service", "provider", id, beanDefinition);
//        parseNested(element, parserContext, ReferenceBean.class, false, "reference", "consumer", id, beanDefinition);
    private static void parseNested(Element element, ParserContext parserContext, Class<?> beanClass, boolean required, String tag, String property, String ref, BeanDefinition beanDefinition) {
        NodeList nodeList = element.getChildNodes();
        if (nodeList != null && nodeList.getLength() > 0) {
            boolean first = true;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    if (tag.equals(node.getNodeName())
                            || tag.equals(node.getLocalName())) {
                        //查找第一个符合的配置，查看呢，然后看最初是否设置default，没有则设置default=false，说明使用内部的定义，不使用默认的
                        if (first) {
                            first = false;
                            String isDefault = element.getAttribute("default");
                            if (isDefault == null || isDefault.length() == 0) {
                                beanDefinition.getPropertyValues().addPropertyValue("default", "false");
                            }
                        }
                        //递归解析
                        BeanDefinition subDefinition = parse((Element) node, parserContext, beanClass, required);
                        if (subDefinition != null && ref != null && ref.length() > 0) {
                            //为嵌套的bean添加外部引用，比如
                            //    <dubbo:provider default="false">
                            //        <dubbo:service interface="xxxxx" ref="xxxxxx"/>
                            //    </dubbo:provider>
                            subDefinition.getPropertyValues().addPropertyValue(property, new RuntimeBeanReference(ref));
                        }
                    }
                }
            }
        }
    }

    /**
     * 解析属性
     *
     * @param nodeList
     * @param beanDefinition
     */
    private static void parseProperties(NodeList nodeList, RootBeanDefinition beanDefinition) {
//        //超级简单，只能这几种格式
//
//            <dubbo:service interface="com.alibaba.dubbo.config.spring.api.DemoService" class="xxxxx" >
//                <property name="xxxx" value="xxxx"/>
//                <property name="xxxxxx" ref="xxxxxx"/>
//            </dubbo:service>
        if (nodeList != null && nodeList.getLength() > 0) {
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    if ("property".equals(node.getNodeName())
                            || "property".equals(node.getLocalName())) {
                        String name = ((Element) node).getAttribute("name");
                        if (name != null && name.length() > 0) {
                            String value = ((Element) node).getAttribute("value");
                            String ref = ((Element) node).getAttribute("ref");
                            if (value != null && value.length() > 0) {
                                beanDefinition.getPropertyValues().addPropertyValue(name, value);
                            } else if (ref != null && ref.length() > 0) {
                                beanDefinition.getPropertyValues().addPropertyValue(name, new RuntimeBeanReference(ref));
                            } else {
                                throw new UnsupportedOperationException("Unsupported <property name=\"" + name + "\"> sub tag, Only supported <property name=\"" + name + "\" ref=\"...\" /> or <property name=\"" + name + "\" value=\"...\" />");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 解析参数
     * @param nodeList
     * @param beanDefinition
     * @return
     */
    @SuppressWarnings("unchecked")
    private static ManagedMap parseParameters(NodeList nodeList, RootBeanDefinition beanDefinition) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedMap parameters = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    //遍历所有parameter的节点
                    if ("parameter".equals(node.getNodeName())
                            || "parameter".equals(node.getLocalName())) {
                        if (parameters == null) {
                            parameters = new ManagedMap();
                        }
                        //获得key
                        String key = ((Element) node).getAttribute("key");
                        //获得value
                        String value = ((Element) node).getAttribute("value");
                        //是否是隐藏字段
                        boolean hide = "true".equals(((Element) node).getAttribute("hide"));
                        if (hide) {
                            key = Constants.HIDE_KEY_PREFIX + key;
                        }
                        //隐藏字段+.前缀
                        parameters.put(key, new TypedStringValue(value, String.class));
                    }
                }
            }
            return parameters;
        }
        return null;
    }

    /**
     * 解析参数
     * @param id
     * @param nodeList
     * @param beanDefinition
     * @param parserContext
     */
    @SuppressWarnings("unchecked")
    private static void parseMethods(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                     ParserContext parserContext) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedList methods = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    Element element = (Element) node;
                    //遍历所有method的节点
                    if ("method".equals(node.getNodeName()) || "method".equals(node.getLocalName())) {
                        //获得name属性，method必须有，应该从xsd上来规范，事实上已经规范了
                        String methodName = element.getAttribute("name");
                        if (methodName == null || methodName.length() == 0) {
                            throw new IllegalStateException("<dubbo:method> name attribute == null");
                        }
                        if (methods == null) {
                            methods = new ManagedList();
                        }
                        //嵌套解析获得bean定义
                        BeanDefinition methodBeanDefinition = parse(((Element) node),
                                parserContext, MethodConfig.class, false);
                        String name = id + "." + methodName;
                        BeanDefinitionHolder methodBeanDefinitionHolder = new BeanDefinitionHolder(
                                methodBeanDefinition, name);
                        //加入
                        methods.add(methodBeanDefinitionHolder);
                    }
                }
            }
            if (methods != null) {
                beanDefinition.getPropertyValues().addPropertyValue("methods", methods);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void parseArguments(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                       ParserContext parserContext) {
        if (nodeList != null && nodeList.getLength() > 0) {
            ManagedList arguments = null;
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node instanceof Element) {
                    Element element = (Element) node;
                    //遍历所有argument的节点
                    if ("argument".equals(node.getNodeName()) || "argument".equals(node.getLocalName())) {
                        String argumentIndex = element.getAttribute("index");
                        if (arguments == null) {
                            arguments = new ManagedList();
                        }
                        BeanDefinition argumentBeanDefinition = parse(((Element) node),
                                parserContext, ArgumentConfig.class, false);
                        String name = id + "." + argumentIndex;
                        BeanDefinitionHolder argumentBeanDefinitionHolder = new BeanDefinitionHolder(
                                argumentBeanDefinition, name);
                        arguments.add(argumentBeanDefinitionHolder);
                    }
                }
            }
            if (arguments != null) {
                beanDefinition.getPropertyValues().addPropertyValue("arguments", arguments);
            }
        }
    }

}