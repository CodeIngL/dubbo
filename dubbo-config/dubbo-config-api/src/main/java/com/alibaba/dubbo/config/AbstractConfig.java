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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.support.Parameter;

import static com.alibaba.dubbo.common.Constants.DEFAULT_KEY;
import static com.alibaba.dubbo.common.utils.StringUtils.isBlank;
import static com.alibaba.dubbo.common.utils.StringUtils.isEmpty;
import static com.alibaba.dubbo.common.utils.StringUtils.isNotEmpty;
import static java.lang.reflect.Modifier.isPublic;

/**
 * 配置解析的工具方法、公共方法
 *
 * @author william.liangf
 * @export
 */
public abstract class AbstractConfig implements Serializable {

    private static final long serialVersionUID = 4267533505537413570L;

    protected static final Logger logger = LoggerFactory.getLogger(AbstractConfig.class);

    private static final int MAX_LENGTH = 100;

    private static final int MAX_PATH_LENGTH = 200;

    private static final Pattern PATTERN_NAME = Pattern.compile("[\\-._0-9a-zA-Z]+");

    private static final Pattern PATTERN_MULTI_NAME = Pattern.compile("[,\\-._0-9a-zA-Z]+");

    private static final Pattern PATTERN_METHOD_NAME = Pattern.compile("[a-zA-Z][0-9a-zA-Z]*");

    private static final Pattern PATTERN_PATH = Pattern.compile("[/\\-$._0-9a-zA-Z]+");

    private static final Pattern PATTERN_NAME_HAS_SYMBOL = Pattern.compile("[:*,/\\-._0-9a-zA-Z]+");

    private static final Pattern PATTERN_KEY = Pattern.compile("[*,\\-._0-9a-zA-Z]+");

    protected String id;

    @Parameter(excluded = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    protected void appendAnnotation(Class<?> annotationClass, Object annotation) {
        Method[] methods = annotationClass.getMethods();
        for (Method method : methods) {
            if (method.getDeclaringClass() != Object.class
                    && method.getReturnType() != void.class
                    && method.getParameterTypes().length == 0
                    && isPublic(method.getModifiers())
                    && !Modifier.isStatic(method.getModifiers())) {
                try {
                    String property = method.getName();
                    if ("interfaceClass".equals(property) || "interfaceName".equals(property)) {
                        property = "interface";
                    }
                    String setter = "set" + property.substring(0, 1).toUpperCase() + property.substring(1);
                    Object value = method.invoke(annotation, new Object[0]);
                    if (value != null && !value.equals(method.getDefaultValue())) {
                        Class<?> parameterType = ReflectUtils.getBoxedClass(method.getReturnType());
                        if ("filter".equals(property) || "listener".equals(property)) {
                            parameterType = String.class;
                            value = StringUtils.join((String[]) value, ",");
                        } else if ("parameters".equals(property)) {
                            parameterType = Map.class;
                            value = CollectionUtils.toStringMap((String[]) value);
                        }
                        try {
                            Method setterMethod = getClass().getMethod(setter, new Class<?>[]{parameterType});
                            setterMethod.invoke(this, new Object[]{value});
                        } catch (NoSuchMethodException e) {
                            // ignore
                        }
                    }
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * <p>
     * 完成对配置类基本属性的设置<br/>
     * 基本思路调用基本属性（基本类型对应类）暴露出来的set方法完成对基本属性的设置<br/>
     * 对于某个基本属性的设置，无论该属性是否已经被设置过，都会重新被设置（系统中存在的话）</br>
     * 上述情况不发生的话，对于某个基本属性的设置，如果已经被设置，就不会触发配置，也就是说配置文件也无效了</br>
     * ConfigUtils.getProperty(name)该方法也会先从系统先获取。再尝试从配置文件中获取<br/>
     * <ul>
     * <li>获得前缀prefix：config equal to ServiceConfig(ServiceBean) and result TagName is service and the prefix equal to "dubbo.service."</li><br/>
     * <li>获得后缀suffix: config has method named setStudentName and result the suffix equal to "student-name"</li><br/>
     * <li>尝试获得value: 使用System.getProperty(prefix + config.id + "." + suffix)获得</li><br/>
     * <li>尝试获得value: 使用System.getProperty(prefix + suffix)获得</li><br/>
     * <li>尝试获得value: 基本属性对应get or is方法获得是空值。使用ConfigUtils.getProperty(prefix + suffix)获得</li><br/>
     * <li>尝试获得value: 基本属性对应get or is方法获得是空值。使用ConfigUtils.getProperty(prefix + config.id + "." + suffix)获得</li><br/>
     * <li>尝试获得value: 基本属性对应get or is方法获得是空值。使用legacyProperties和ConfigUtils.getProperty()获得</li><br/>
     * <ul>
     * </p>
     *
     * @param config 配置类（简单配置类or复杂配置类）
     * @apiNote
     * @see ConfigUtils
     * @see ConfigUtils#getProperty(String)
     */
    protected static void appendProperties(AbstractConfig config) {
        if (config == null) {
            return;
        }
        //系统参数属性的前缀
        String prefix = "dubbo." + getTagName(config.getClass()) + ".";

        //遍历传入对象的方法
        Method[] methods = config.getClass().getMethods();
        for (Method method : methods) {
            try {
                String name = method.getName();
                //暴露的基本属性set方法
                if (name.length() > 3 && name.startsWith("set") && isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 1 && isPrimitive(method.getParameterTypes()[0])) {
                    //方法名转换. ex: setStudentName will transform to "student-name"
                    String suffix = StringUtils.camelToSplitName(name.substring(3, 4).toLowerCase() + name.substring(4), "-");

                    String idPn = prefix + config.getId() + "." + suffix;
                    String pn = prefix + suffix;
                    boolean hasId = !idPn.equals(pn);

                    //设置config对象属性的值，上面出现了一个id，是为了区分同一类型不同配置对象。如果id没有则使用，针对同一类型的配置
                    //系统属性为最高优先级

                    String value = System.getProperty(idPn);
                    if (isBlank(value) && hasId) {
                        value = System.getProperty(pn);
                    }
                    if (!isBlank(value)) {
                        logger.info("Use System Property " + pn + " to config dubbo");
                        method.invoke(config, new Object[]{convertPrimitive(method.getParameterTypes()[0], value)});
                        return;
                    }

                    //检测本身已经有值，本身的优先，通过get或者Is来获得值，有值，我们不需要操作了
                    Method getter;
                    Class configCls = config.getClass();
                    try {
                        getter = configCls.getMethod("get" + name.substring(3), new Class<?>[0]);
                    } catch (NoSuchMethodException e) {
                        try {
                            getter = configCls.getMethod("is" + name.substring(3), new Class<?>[0]);
                        } catch (NoSuchMethodException e2) {
                            getter = null;
                        }
                    }
                    if (getter == null) {
                        return;
                    }
                    if (getter.invoke(config, new Object[0]) != null) { //已经有值了
                        return;
                    }

                    //从配置文件中获得对应值
                    value = ConfigUtils.getProperty(idPn);
                    if (isBlank(value) && hasId) {
                        value = ConfigUtils.getProperty(pn);
                    }
                    if (isBlank(value)) {
                        return;
                    }
                    method.invoke(config, new Object[]{convertPrimitive(method.getParameterTypes()[0], value)});
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 获得配置类的标识,
     * 尝试去掉后缀Config或者Bean
     * <p>如果cls等于DemoConfig或DemoBean，它将返回demo<br/>
     * 如果cls等于DemoSimple，它将返回demosimple
     * </p>
     *
     * @param cls 配置类
     * @return 标识
     */
    private static String getTagName(Class<?> cls) {
        String tag = cls.getSimpleName();
        for (String suffix : SUFFIXS) {
            if (tag.endsWith(suffix)) {
                tag = tag.substring(0, tag.length() - suffix.length());
                break;
            }
        }
        tag = tag.toLowerCase();
        return tag;
    }

    /**
     * 将config中一些信息形成键值对，加入入参parameters中<br/>
     * it equals to appendParameters(parameters, config, null)
     *
     * @param parameters 参数集合
     * @param config     配置类实例
     * @see #appendAttributes(Map, Object, String)
     */
    protected static void appendParameters(Map<String, String> parameters, Object config) {
        appendParameters(parameters, config, null);
    }

    /**
     * <p>
     * 获取config中的基本类型字段，通过其暴露的get or is 方法，其中去掉了某些特殊方法，形成键值对放入parameter中
     * <ul>
     * <li>获得方法注解@Parameter，排除注解excluded=true的方法</li><br/>
     * <li>排除返回值为Object的方法</li><br/>
     * <li>获得键名:优先使用@Parameter的key，其次使用方法名转换，ex:getStudentName will transforms to "student.name"。如果有前缀需要加前缀</li><br/>
     * <li>获得值值:反射调用方法，string化，根据@Parameter的escaped来编码，根据@Parameter的append来追加前缀</li><br/>
     * <li>放入键值对</li><br/>
     * </ul>
     * </p>
     * <p>
     * 处理getParameters方法
     * <ul>
     * <li>根据有无前缀为key追加前缀</li>
     * <li>替换key中"-"符号,该符号由{@link #appendProperties(AbstractConfig)}处理引起</li>
     * </ul>
     * </p>
     *
     * @param parameters 结果集合
     * @param config     对象
     * @param prefix     前缀
     */
    @SuppressWarnings("unchecked")
    protected static void appendParameters(Map<String, String> parameters, Object config, String prefix) {
        if (config == null) {
            return;
        }
        Method[] methods = config.getClass().getMethods();
        for (Method method : methods) {//遍历对象方法
            try {
                String methodName = method.getName();
                //筛选get.is.除掉getClass，公有，无参，返回是基本的类型，或者某些特定类型
                if ((methodName.startsWith("get") || methodName.startsWith("is"))
                        && !"getClass".equals(methodName)
                        && isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 0
                        && isPrimitive(method.getReturnType())) {
                    Parameter parameter = method.getAnnotation(Parameter.class);//获取方法上注解
                    if (method.getReturnType() == Object.class || parameter != null && parameter.excluded()) {
                        continue;//返回是Object，或者注解指定要排除的，不需要被加入
                    }
                    String key = null;
                    if (parameter != null) {
                        key = parameter.key().trim();//注解指定的，直接使用注解指定的
                    }
                    if (isEmpty(key)) {
                        int i = methodName.startsWith("get") ? 3 : 2;//尝试从相关的get方法中提取名称
                        key = StringUtils.camelToSplitName(methodName.substring(i, i + 1).toLowerCase() + methodName.substring(i + 1), ".");//方法名变成xxx.xxx.xxx
                    }
                    Object value = method.invoke(config, new Object[0]);//获得get方法的返回值
                    String str = String.valueOf(value).trim();//字符串化
                    if (value != null && str.length() > 0) {
                        if (parameter != null && parameter.escaped()) {//设置了escaped，编码
                            str = URL.encode(str);
                        }
                        if (parameter != null && parameter.append()) {//设置了append，使用追加的方式,为值增加其他配置
                            String pre = parameters.get(DEFAULT_KEY + "." + key);
                            if (isNotEmpty(pre)) {
                                str = pre + "," + str;
                            }
                            pre = parameters.get(key);
                            if (isNotEmpty(pre)) {
                                str = pre + "," + str;
                            }
                        }
                        if (isNotEmpty(prefix)) {//有前缀，为键追加前缀
                            key = prefix + "." + key;
                        }
                        parameters.put(key, str);//放置键值对
                    } else if (parameter != null && parameter.required()) {
                        throw new IllegalStateException(config.getClass().getSimpleName() + "." + key + " == null");
                    }
                }
                //筛选getParameters方法，返回值是Map，公有，无参
                else if ("getParameters".equals(methodName)
                        && isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 0
                        && method.getReturnType() == Map.class) {
                    Map<String, String> map = (Map<String, String>) method.invoke(config, new Object[0]);//获得getParameters返回值
                    if (map != null && map.size() > 0) {
                        String pre = (isNotEmpty(prefix) ? prefix + "." : "");
                        for (Map.Entry<String, String> entry : map.entrySet()) {
                            parameters.put(pre + entry.getKey().replace('-', '.'), entry.getValue());//放入parameter中
                        }
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    protected static void appendAttributes(Map<Object, Object> parameters, Object config) {
        appendAttributes(parameters, config, null);
    }

    /**
     * 无效注释
     * <p>
     * 获取config中的基本类型字段，通过其暴露的get or is 方法，其中去掉了某些特殊方法，形成键值对放入parameter中
     * <ul>
     * <li>获得方法注解@Parameter，排除注解excluded=true的方法</li><br/>
     * <li>排除返回值为Object的方法</li><br/>
     * <li>获得键名:优先使用@Parameter的key，其次使用方法名转换，ex:getStudentName will transforms to "student.name"。如果有前缀需要加前缀</li><br/>
     * <li>获得值值:反射调用方法，string化，根据@Parameter的escaped来编码，根据@Parameter的append来追加前缀</li><br/>
     * <li>放入键值对</li><br/>
     * </ul>
     * </p>
     * <p>
     * 处理getParameters方法
     * <ul>
     * <li>根据有无前缀为key追加前缀</li>
     * <li>替换key中"-"符号,该符号由{@link #appendProperties(AbstractConfig)}处理引起</li>
     * </ul>
     * </p>
     *
     * @param parameters
     * @param config
     * @param prefix
     */
    protected static void appendAttributes(Map<Object, Object> parameters, Object config, String prefix) {
        if (config == null) {
            return;
        }
        Method[] methods = config.getClass().getMethods();
        for (Method method : methods) {
            try {
                String name = method.getName();
                if ((name.startsWith("get") || name.startsWith("is"))
                        && !"getClass".equals(name)
                        && isPublic(method.getModifiers())
                        && method.getParameterTypes().length == 0
                        && isPrimitive(method.getReturnType())) {
                    Parameter parameter = method.getAnnotation(Parameter.class);
                    if (parameter == null || !parameter.attribute())
                        continue;
                    String key = parameter.key().trim();
                    if (key.length() == 0) {
                        int i = name.startsWith("get") ? 3 : 2;
                        key = name.substring(i, i + 1).toLowerCase() + name.substring(i + 1);
                    }
                    Object value = method.invoke(config, new Object[0]);
                    if (value != null) {
                        if (prefix != null && prefix.length() > 0) {
                            key = prefix + "." + key;
                        }
                        parameters.put(key, value);
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    private static boolean isPrimitive(Class<?> type) {
        return type.isPrimitive()
                || type == String.class
                || type == Character.class
                || type == Boolean.class
                || type == Byte.class
                || type == Short.class
                || type == Integer.class
                || type == Long.class
                || type == Float.class
                || type == Double.class
                || type == Object.class;
    }

    private static Object convertPrimitive(Class<?> type, String value) {
        if (type == char.class || type == Character.class) {
            return value.length() > 0 ? value.charAt(0) : '\0';
        } else if (type == boolean.class || type == Boolean.class) {
            return Boolean.valueOf(value);
        } else if (type == byte.class || type == Byte.class) {
            return Byte.valueOf(value);
        } else if (type == short.class || type == Short.class) {
            return Short.valueOf(value);
        } else if (type == int.class || type == Integer.class) {
            return Integer.valueOf(value);
        } else if (type == long.class || type == Long.class) {
            return Long.valueOf(value);
        } else if (type == float.class || type == Float.class) {
            return Float.valueOf(value);
        } else if (type == double.class || type == Double.class) {
            return Double.valueOf(value);
        }
        return value;
    }

    protected static void checkExtension(Class<?> type, String property, String value) {
        checkName(property, value);
        if (value != null && value.length() > 0
                && !ExtensionLoader.getExtensionLoader(type).hasExtension(value)) {
            throw new IllegalStateException("No such extension " + value + " for " + property + "/" + type.getName());
        }
    }

    protected static void checkMultiExtension(Class<?> type, String property, String value) {
        checkMultiName(property, value);
        if (value != null && value.length() > 0) {
            String[] values = value.split("\\s*[,]+\\s*");
            for (String v : values) {
                if (v.startsWith(Constants.REMOVE_VALUE_PREFIX)) {
                    v = v.substring(1);
                }
                if (DEFAULT_KEY.equals(v)) {
                    continue;
                }
                if (!ExtensionLoader.getExtensionLoader(type).hasExtension(v)) {
                    throw new IllegalStateException("No such extension " + v + " for " + property + "/" + type.getName());
                }
            }
        }
    }

    protected static void checkLength(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, null);
    }

    protected static void checkPathLength(String property, String value) {
        checkProperty(property, value, MAX_PATH_LENGTH, null);
    }

    protected static void checkName(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_NAME);
    }

    protected static void checkNameHasSymbol(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_NAME_HAS_SYMBOL);
    }

    protected static void checkKey(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_KEY);
    }

    protected static void checkMultiName(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_MULTI_NAME);
    }

    protected static void checkPathName(String property, String value) {
        checkProperty(property, value, MAX_PATH_LENGTH, PATTERN_PATH);
    }

    protected static void checkMethodName(String property, String value) {
        checkProperty(property, value, MAX_LENGTH, PATTERN_METHOD_NAME);
    }

    protected static void checkParameterName(Map<String, String> parameters) {
        if (parameters == null || parameters.size() == 0) {
            return;
        }
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            //change by tony.chenl parameter value maybe has colon.for example napoli address
            checkNameHasSymbol(entry.getKey(), entry.getValue());
        }
    }

    protected static void checkProperty(String property, String value, int maxlength, Pattern pattern) {
        if (value == null || value.length() == 0) {
            return;
        }
        if (value.length() > maxlength) {
            throw new IllegalStateException("Invalid " + property + "=\"" + value + "\" is longer than " + maxlength);
        }
        if (pattern != null) {
            Matcher matcher = pattern.matcher(value);
            if (!matcher.matches()) {
                throw new IllegalStateException("Invalid " + property + "=\"" + value + "\" contain illegal charactor, only digit, letter, '-', '_' and '.' is legal.");
            }
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                if (logger.isInfoEnabled()) {
                    logger.info("Run shutdown hook now.");
                }
                ProtocolConfig.destroyAll();
            }
        }, "DubboShutdownHook"));
    }

    private static final String[] SUFFIXS = new String[]{"Config", "Bean"};

    @Override
    public String toString() {
        try {
            StringBuilder buf = new StringBuilder();
            buf.append("<dubbo:").append(getTagName(getClass()));
            for (Method method : getClass().getMethods()) {
                try {
                    String methodName = method.getName();
                    if ((methodName.startsWith("get") || methodName.startsWith("is"))
                            && !"getClass".equals(methodName) && !"get".equals(methodName) && !"is".equals(methodName)
                            && isPublic(method.getModifiers())
                            && method.getParameterTypes().length == 0
                            && isPrimitive(method.getReturnType())) {
                        int i = methodName.startsWith("get") ? 3 : 2;
                        String key = methodName.substring(i, i + 1).toLowerCase() + methodName.substring(i + 1);
                        Object value = method.invoke(this, new Object[0]);
                        if (value != null) {
                            buf.append(" ").append(key).append("=\"").append(value).append("\"");
                        }
                    }
                } catch (Exception e) {
                    logger.warn(e.getMessage(), e);
                }
            }
            buf.append(" />");
            return buf.toString();
        } catch (Throwable t) { // 防御性容错
            logger.warn(t.getMessage(), t);
            return super.toString();
        }
    }


}