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

/**
 * ConsumerConfig(消费方的默认的模板配置)
 * <p>
 * 在具体消费应用的ReferenceConfig没有配置相关属性时，总会尝试使用该模板作为配置。<br/>
 * 具体表现为下面几个方面:
 * <ul>
 * <li>消费方引用具体的ReferenceConfig，没用配置相关嵌套的配置类，尝试使用该模板类的持有嵌套配置类，来实现默认的配置</li><br/>
 * <li>消费方引用构建具体的url参数集合时，该模板配置类，相关信息写入参数集合，前缀为default，作为默认值</li><br/>
 * <li>一个特别的属性:消费方的check没有配置的时候，使用该模板配置类的check属性作为默认值</li><br/>
 * <li>一个特别的属性:消费方的generic没有配置的时候，使用该模板配置类的generic属性作为默认值</li><br/>
 * </ul>
 * </p>
 *
 * @author william.liangf
 * @export
 */
public class ConsumerConfig extends AbstractReferenceConfig {

    private static final long serialVersionUID = 2827274711143680600L;

    // 是否为缺省
    private Boolean isDefault;

    @Override
    public void setTimeout(Integer timeout) {
        super.setTimeout(timeout);
        String rmiTimeout = System.getProperty("sun.rmi.transport.tcp.responseTimeout");
        if (timeout != null && timeout > 0 && (rmiTimeout == null || rmiTimeout.length() == 0)) {
            System.setProperty("sun.rmi.transport.tcp.responseTimeout", String.valueOf(timeout));
        }
    }

    public Boolean isDefault() {
        return isDefault;
    }

    public void setDefault(Boolean isDefault) {
        this.isDefault = isDefault;
    }

}