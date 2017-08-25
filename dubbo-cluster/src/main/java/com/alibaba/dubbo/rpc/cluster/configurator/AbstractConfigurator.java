/*
 * Copyright 1999-2012 Alibaba Group.
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
package com.alibaba.dubbo.rpc.cluster.configurator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.cluster.Configurator;

/**
 * AbstractOverrideConfigurator
 *
 * @author william.liangf
 */
public abstract class AbstractConfigurator implements Configurator {

    //配置的url
    private final URL configuratorUrl;

    /**
     * url配置抽象构造
     * @param url 可配置的url
     */
    public AbstractConfigurator(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("configurator url == null");
        }
        this.configuratorUrl = url;
    }

    public URL getUrl() {
        return configuratorUrl;
    }

    /**
     * 配置url
     *
     * @param url - old provider url.
     * @return 新的提供url
     */
    public URL configure(URL url) {
        //校验本身可配置url和入参url
        if (configuratorUrl == null || configuratorUrl.getHost() == null || url == null || url.getHost() == null) {
            return url;
        }
        //可配置的url代表了任意，或者入参和可配置严格相等的情况下，才重新配置
        if (Constants.ANYHOST_VALUE.equals(configuratorUrl.getHost()) || url.getHost().equals(configuratorUrl.getHost())) {
            //获得可配置url的application，默认是名字
            String configApplication = configuratorUrl.getParameter(Constants.APPLICATION_KEY, configuratorUrl.getUsername());
            //获得入参url的application，默认是名字
            String currentApplication = url.getParameter(Constants.APPLICATION_KEY, url.getUsername());
            //可配置的application无效或者通配或者两者严格相等的情况下
            if (configApplication == null || Constants.ANY_VALUE.equals(configApplication) || configApplication.equals(currentApplication)) {
                //可配置的端口无效，或者两者严格相等的情况下
                if (configuratorUrl.getPort() == 0 || url.getPort() == configuratorUrl.getPort()) {
                    //创建代表状态的集合:category，check，dynamic，enabled
                    Set<String> condtionKeys = new HashSet<String>();
                    condtionKeys.add(Constants.CATEGORY_KEY);
                    condtionKeys.add(Constants.CHECK_KEY);
                    condtionKeys.add(Constants.DYNAMIC_KEY);
                    condtionKeys.add(Constants.ENABLED_KEY);
                    //筛选可配置的url中的信息
                    for (Map.Entry<String, String> entry : configuratorUrl.getParameters().entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        if (key.startsWith("~") || Constants.APPLICATION_KEY.equals(key) || Constants.SIDE_KEY.equals(key)) {
                            //处理~开头的，或者application，或者side；
                            condtionKeys.add(key);
                            if (value != null && !Constants.ANY_VALUE.equals(value) && !value.equals(url.getParameter(key.startsWith("~") ? key.substring(1) : key))) {
                                //一旦双方信息对不上返回入参的url
                                return url;
                            }
                        }
                    }
                    //回调具体的实现
                    return doConfigure(url, configuratorUrl.removeParameters(condtionKeys));
                }
            }
        }
        return url;
    }

    public int compareTo(Configurator o) {
        if (o == null) {
            return -1;
        }
        return getUrl().getHost().compareTo(o.getUrl().getHost());
    }

    protected abstract URL doConfigure(URL currentUrl, URL configUrl);

    public static void main(String[] args) {
        System.out.println(URL.encode("timeout=100"));
    }

}
