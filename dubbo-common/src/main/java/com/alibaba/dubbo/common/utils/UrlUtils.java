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
package com.alibaba.dubbo.common.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;

import static com.alibaba.dubbo.common.Constants.REGISTRY_SPLIT_PATTERN;

public class UrlUtils {


    /**
     * <p>
     * 解析字符串到URL的列表<br/>
     * ex: <br/>
     * <ul>
     * <li>zookeeper://127.0.0.1:2181</li><br/>
     * <li>127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183</li><br/>
     * </ul>
     * 处理逻辑
     * <ul>
     * <li>根据是否有://符号来，有url直接为地址</li>
     * <li>没有://符号,处理分割符“，”对于多个地址，第一个地址为url，其他追加到为backup上</li>
     * <li>从defaults中获得protocol，username，password，port，path</li>
     * <li>根据url字符串解析为URL对象</li>
     * <li>对于URL对象没有配置的属性，使用defaults中的配置进行配置，主要是protocol，username，password，port，path</li>
     * <li>URL对象拥有的参数集合，与defaults进行合并，合并规则，遍历defaults中键值对，参数集合不存在相应键，就放入</li>
     * </ul>
     * tip:直接配在url中的参数有优先权
     * </p>
     *
     * @param address  地址字符串
     * @param defaults 默认参数集合，当前address不存在相关信息是生效， 回进行浅copy
     * @return 新URL
     * @see URL#valueOf(String)
     */
    public static URL parseURL(String address, Map<String, String> defaults) {
        if (address == null || address.length() == 0) {
            return null;
        }
        String url;
        if (address.indexOf("://") >= 0) {
            //address like zookeeper://127.0.0.1:2181
            //url equal to zookeeper://127.0.0.1:2181
            url = address;
        } else {
            //address like 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183
            //url equal to 127.0.0.1:2181?backup=127.0.0.1:2182,127.0.0.1:2183
            String[] addresses = Constants.COMMA_SPLIT_PATTERN.split(address);
            url = addresses[0];
            if (addresses.length > 1) {
                StringBuilder backup = new StringBuilder();
                for (int i = 1; i < addresses.length; i++) {
                    if (i > 1) {
                        backup.append(",");
                    }
                    backup.append(addresses[i]);
                }
                url += "?" + Constants.BACKUP_KEY + "=" + backup.toString();
            }
        }

        //从url字符串解析为URL对象中解析
        boolean changed = false;
        URL u = URL.valueOf(url);
        //获得protocol
        String protocol = u.getProtocol();
        //获得username
        String username = u.getUsername();
        //获得password
        String password = u.getPassword();
        //获得host
        String host = u.getHost();
        //获得port
        int port = u.getPort();
        //获得path
        String path = u.getPath();
        //获得url中的参数集合
        Map<String, String> parameters = new HashMap<String, String>(u.getParameters());

        Map<String, String> defaultParameters = defaults == null ? null : new HashMap<String, String>(defaults);
        String defaultProtocol = null, defaultUsername = null, defaultPassword = null, defaultPath = null;
        int defaultPort = 0;
        if (defaultParameters != null) {
            defaultParameters.remove("host");
            defaultProtocol = defaultParameters.remove("protocol");
            defaultUsername = defaultParameters.remove("username");
            defaultPassword = defaultParameters.remove("password");
            defaultPort = StringUtils.parseInteger(defaultParameters.remove("port"));
            defaultPath = defaultParameters.remove("path");
            for (Map.Entry<String, String> entry : defaultParameters.entrySet()) {
                String key = entry.getKey();
                String defaultValue = entry.getValue();
                if (StringUtils.isNotEmpty(defaultValue) && StringUtils.isEmpty(parameters.get(key))) {
                    changed = true;
                    parameters.put(key, defaultValue);
                }
            }
        }
        if ((StringUtils.isEmpty(protocol))) {
            changed = true;
            if (StringUtils.isEmpty(defaultProtocol)) {
                defaultProtocol = "dubbo";
            }
            protocol = defaultProtocol;
        }
        if ((StringUtils.isEmpty(username)) && StringUtils.isNotEmpty(defaultUsername)) {
            changed = true;
            username = defaultUsername;
        }
        if ((StringUtils.isEmpty(password)) && StringUtils.isNotEmpty(defaultPassword)) {
            changed = true;
            password = defaultPassword;
        }
        if (port <= 0) {
            if (defaultPort > 0) {
                changed = true;
                port = defaultPort;
            } else {
                changed = true;
                port = 9090;
            }
        }
        if ((StringUtils.isEmpty(path)) && StringUtils.isNotEmpty(defaultPath)) {
            changed = true;
            path = defaultPath;
        }
        if (changed) {
            u = new URL(protocol, username, password, host, port, path, parameters);
        }
        return u;
    }

    /**
     * <p>
     * 解析字符串到URL的列表<br/>
     * ex:address like zookeeper://127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183|redis://127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381<br/>
     * result: address["zookeeper://127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183","redis://127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381"]
     * </p>
     * <p>值得说明的是分割符为”|;“，通常中间件的集群url直接使用逗号分开，因而没有冲突</p>
     *
     * @param address  地址字符串
     * @param defaults url参数map默认值
     * @return URL对象列表
     * @see UrlUtils#parseURL(String, Map)
     */
    public static List<URL> parseURLs(String address, Map<String, String> defaults) {
        if (address == null || address.length() == 0) {
            return null;
        }
        //"|或者符合;"拆分
        String[] addresses = REGISTRY_SPLIT_PATTERN.split(address);
        List<URL> registries = new ArrayList<URL>();
        for (String addr : addresses) {
            registries.add(parseURL(addr, defaults));
        }
        return registries;
    }

    public static Map<String, Map<String, String>> convertRegister(Map<String, Map<String, String>> register) {
        Map<String, Map<String, String>> newRegister = new HashMap<String, Map<String, String>>();
        for (Map.Entry<String, Map<String, String>> entry : register.entrySet()) {
            String serviceName = entry.getKey();
            Map<String, String> serviceUrls = entry.getValue();
            if (!serviceName.contains(":") && !serviceName.contains("/")) {
                for (Map.Entry<String, String> entry2 : serviceUrls.entrySet()) {
                    String serviceUrl = entry2.getKey();
                    String serviceQuery = entry2.getValue();
                    Map<String, String> params = StringUtils.parseQueryString(serviceQuery);
                    String group = params.get("group");
                    String version = params.get("version");
                    //params.remove("group");
                    //params.remove("version");
                    String name = serviceName;
                    if (group != null && group.length() > 0) {
                        name = group + "/" + name;
                    }
                    if (version != null && version.length() > 0) {
                        name = name + ":" + version;
                    }
                    Map<String, String> newUrls = newRegister.get(name);
                    if (newUrls == null) {
                        newUrls = new HashMap<String, String>();
                        newRegister.put(name, newUrls);
                    }
                    newUrls.put(serviceUrl, StringUtils.toQueryString(params));
                }
            } else {
                newRegister.put(serviceName, serviceUrls);
            }
        }
        return newRegister;
    }

    public static Map<String, String> convertSubscribe(Map<String, String> subscribe) {
        Map<String, String> newSubscribe = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : subscribe.entrySet()) {
            String serviceName = entry.getKey();
            String serviceQuery = entry.getValue();
            if (!serviceName.contains(":") && !serviceName.contains("/")) {
                Map<String, String> params = StringUtils.parseQueryString(serviceQuery);
                String group = params.get("group");
                String version = params.get("version");
                //params.remove("group");
                //params.remove("version");
                String name = serviceName;
                if (group != null && group.length() > 0) {
                    name = group + "/" + name;
                }
                if (version != null && version.length() > 0) {
                    name = name + ":" + version;
                }
                newSubscribe.put(name, StringUtils.toQueryString(params));
            } else {
                newSubscribe.put(serviceName, serviceQuery);
            }
        }
        return newSubscribe;
    }

    public static Map<String, Map<String, String>> revertRegister(Map<String, Map<String, String>> register) {
        Map<String, Map<String, String>> newRegister = new HashMap<String, Map<String, String>>();
        for (Map.Entry<String, Map<String, String>> entry : register.entrySet()) {
            String serviceName = entry.getKey();
            Map<String, String> serviceUrls = entry.getValue();
            if (serviceName.contains(":") || serviceName.contains("/")) {
                for (Map.Entry<String, String> entry2 : serviceUrls.entrySet()) {
                    String serviceUrl = entry2.getKey();
                    String serviceQuery = entry2.getValue();
                    Map<String, String> params = StringUtils.parseQueryString(serviceQuery);
                    String name = serviceName;
                    int i = name.indexOf('/');
                    if (i >= 0) {
                        params.put("group", name.substring(0, i));
                        name = name.substring(i + 1);
                    }
                    i = name.lastIndexOf(':');
                    if (i >= 0) {
                        params.put("version", name.substring(i + 1));
                        name = name.substring(0, i);
                    }
                    Map<String, String> newUrls = newRegister.get(name);
                    if (newUrls == null) {
                        newUrls = new HashMap<String, String>();
                        newRegister.put(name, newUrls);
                    }
                    newUrls.put(serviceUrl, StringUtils.toQueryString(params));
                }
            } else {
                newRegister.put(serviceName, serviceUrls);
            }
        }
        return newRegister;
    }

    public static Map<String, String> revertSubscribe(Map<String, String> subscribe) {
        Map<String, String> newSubscribe = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : subscribe.entrySet()) {
            String serviceName = entry.getKey();
            String serviceQuery = entry.getValue();
            if (serviceName.contains(":") || serviceName.contains("/")) {
                Map<String, String> params = StringUtils.parseQueryString(serviceQuery);
                String name = serviceName;
                int i = name.indexOf('/');
                if (i >= 0) {
                    params.put("group", name.substring(0, i));
                    name = name.substring(i + 1);
                }
                i = name.lastIndexOf(':');
                if (i >= 0) {
                    params.put("version", name.substring(i + 1));
                    name = name.substring(0, i);
                }
                newSubscribe.put(name, StringUtils.toQueryString(params));
            } else {
                newSubscribe.put(serviceName, serviceQuery);
            }
        }
        return newSubscribe;
    }

    public static Map<String, Map<String, String>> revertNotify(Map<String, Map<String, String>> notify) {
        if (notify != null && notify.size() > 0) {
            Map<String, Map<String, String>> newNotify = new HashMap<String, Map<String, String>>();
            for (Map.Entry<String, Map<String, String>> entry : notify.entrySet()) {
                String serviceName = entry.getKey();
                Map<String, String> serviceUrls = entry.getValue();
                if (!serviceName.contains(":") && !serviceName.contains("/")) {
                    if (serviceUrls != null && serviceUrls.size() > 0) {
                        for (Map.Entry<String, String> entry2 : serviceUrls.entrySet()) {
                            String url = entry2.getKey();
                            String query = entry2.getValue();
                            Map<String, String> params = StringUtils.parseQueryString(query);
                            String group = params.get("group");
                            String version = params.get("version");
                            // params.remove("group");
                            // params.remove("version");
                            String name = serviceName;
                            if (group != null && group.length() > 0) {
                                name = group + "/" + name;
                            }
                            if (version != null && version.length() > 0) {
                                name = name + ":" + version;
                            }
                            Map<String, String> newUrls = newNotify.get(name);
                            if (newUrls == null) {
                                newUrls = new HashMap<String, String>();
                                newNotify.put(name, newUrls);
                            }
                            newUrls.put(url, StringUtils.toQueryString(params));
                        }
                    }
                } else {
                    newNotify.put(serviceName, serviceUrls);
                }
            }
            return newNotify;
        }
        return notify;
    }

    //compatible for dubbo-2.0.0
    public static List<String> revertForbid(List<String> forbid, Set<URL> subscribed) {
        if (forbid != null && forbid.size() > 0) {
            List<String> newForbid = new ArrayList<String>();
            for (String serviceName : forbid) {
                if (!serviceName.contains(":") && !serviceName.contains("/")) {
                    for (URL url : subscribed) {
                        if (serviceName.equals(url.getServiceInterface())) {
                            newForbid.add(url.getServiceKey());
                            break;
                        }
                    }
                } else {
                    newForbid.add(serviceName);
                }
            }
            return newForbid;
        }
        return forbid;
    }

    public static URL getEmptyUrl(String service, String category) {
        String group = null;
        String version = null;
        int i = service.indexOf('/');
        if (i > 0) {
            group = service.substring(0, i);
            service = service.substring(i + 1);
        }
        i = service.lastIndexOf(':');
        if (i > 0) {
            version = service.substring(i + 1);
            service = service.substring(0, i);
        }
        return URL.valueOf(Constants.EMPTY_PROTOCOL + "://0.0.0.0/" + service + "?"
                + Constants.CATEGORY_KEY + "=" + category
                + (group == null ? "" : "&" + Constants.GROUP_KEY + "=" + group)
                + (version == null ? "" : "&" + Constants.VERSION_KEY + "=" + version));
    }

    /**
     * 匹配目录
     * @param category 源目录
     * @param categories 目标目录
     * @return 是否匹配
     */
    public static boolean isMatchCategory(String category, String categories) {
        if (categories == null || categories.length() == 0) {
            //目标目录不存在的情况下，和默认目录进行匹配:providers
            return Constants.DEFAULT_CATEGORY.equals(category);
        } else if (categories.contains(Constants.ANY_VALUE)) {
            //目标目录含有通配符，说明肯定匹配，这里可能是有漏洞的，直接返回true
            return true;
        } else if (categories.contains(Constants.REMOVE_VALUE_PREFIX)) {
            //目标目录含有-
            //则目标目录不含-源目录，返回true
            return !categories.contains(Constants.REMOVE_VALUE_PREFIX + category);
        } else {
            //目标目录含有源目录字符串
            return categories.contains(category);
        }
    }

    /**
     * 对url进行匹配
     * @param consumerUrl 受订阅url
     * @param providerUrl 提供方url
     * @return 是否匹配
     * @see #isMatchCategory(String, String)
     */
    public static boolean isMatch(URL consumerUrl, URL providerUrl) {
        String consumerInterface = consumerUrl.getServiceInterface();
        String providerInterface = providerUrl.getServiceInterface();
        // 检测接口匹配，
        // 不是通配或者严格意义上的相等返回false；
        if (!(Constants.ANY_VALUE.equals(consumerInterface) || StringUtils.isEquals(consumerInterface, providerInterface))) {
            return false;
        }
        //检测目录匹配,
        if (!isMatchCategory(providerUrl.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY),
                consumerUrl.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY))) {
            return false;
        }
        //providerUrl 无效 且 订阅url的enable不为*，直接返回false
        if (!providerUrl.getParameter(Constants.ENABLED_KEY, true)
                && !Constants.ANY_VALUE.equals(consumerUrl.getParameter(Constants.ENABLED_KEY))) {
            return false;
        }

        //版本信息完全一致
        //源url有通配，或者源和目标严格相等
        String consumerGroup = consumerUrl.getParameter(Constants.GROUP_KEY);
        String consumerVersion = consumerUrl.getParameter(Constants.VERSION_KEY);
        String consumerClassifier = consumerUrl.getParameter(Constants.CLASSIFIER_KEY, Constants.ANY_VALUE);

        String providerGroup = providerUrl.getParameter(Constants.GROUP_KEY);
        String providerVersion = providerUrl.getParameter(Constants.VERSION_KEY);
        String providerClassifier = providerUrl.getParameter(Constants.CLASSIFIER_KEY, Constants.ANY_VALUE);
        return (Constants.ANY_VALUE.equals(consumerGroup) || StringUtils.isEquals(consumerGroup, providerGroup) || StringUtils.isContains(consumerGroup, providerGroup))
                && (Constants.ANY_VALUE.equals(consumerVersion) || StringUtils.isEquals(consumerVersion, providerVersion))
                && (consumerClassifier == null || Constants.ANY_VALUE.equals(consumerClassifier) || StringUtils.isEquals(consumerClassifier, providerClassifier));
    }

    public static boolean isMatchGlobPattern(String pattern, String value, URL param) {
        if (param != null && pattern.startsWith("$")) {
            pattern = param.getRawParameter(pattern.substring(1));
        }
        return isMatchGlobPattern(pattern, value);
    }

    public static boolean isMatchGlobPattern(String pattern, String value) {
        if ("*".equals(pattern))
            return true;
        if ((pattern == null || pattern.length() == 0)
                && (value == null || value.length() == 0))
            return true;
        if ((pattern == null || pattern.length() == 0)
                || (value == null || value.length() == 0))
            return false;

        int i = pattern.lastIndexOf('*');
        // 没有找到星号
        if (i == -1) {
            return value.equals(pattern);
        }
        // 星号在末尾
        else if (i == pattern.length() - 1) {
            return value.startsWith(pattern.substring(0, i));
        }
        // 星号的开头
        else if (i == 0) {
            return value.endsWith(pattern.substring(i + 1));
        }
        // 星号的字符串的中间
        else {
            String prefix = pattern.substring(0, i);
            String suffix = pattern.substring(i + 1);
            return value.startsWith(prefix) && value.endsWith(suffix);
        }
    }

    public static boolean isServiceKeyMatch(URL pattern, URL value) {
        return pattern.getParameter(Constants.INTERFACE_KEY).equals(
                value.getParameter(Constants.INTERFACE_KEY))
                && isItemMatch(pattern.getParameter(Constants.GROUP_KEY),
                value.getParameter(Constants.GROUP_KEY))
                && isItemMatch(pattern.getParameter(Constants.VERSION_KEY),
                value.getParameter(Constants.VERSION_KEY));
    }

    /**
     * 判断 value 是否匹配 pattern，pattern 支持 * 通配符.
     *
     * @param pattern pattern
     * @param value   value
     * @return true if match otherwise false
     */
    static boolean isItemMatch(String pattern, String value) {
        if (pattern == null) {
            return value == null;
        } else {
            return "*".equals(pattern) || pattern.equals(value);
        }
    }
}