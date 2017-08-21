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
package com.alibaba.dubbo.rpc.protocol.injvm;

import java.util.Map;

import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.protocol.AbstractExporter;

/**
 * InjvmExporter
 * 
 * @author william.liangf
 */
class InjvmExporter<T> extends AbstractExporter<T> {

    private final String key;
    
    private final Map<String, Exporter<?>> exporterMap;

    /**
     * 内部引用导出对象
     * @param invoker 调用者
     * @param key 服务标识
     * @param exporterMap 缓存
     */
    InjvmExporter(Invoker<T> invoker, String key, Map<String, Exporter<?>> exporterMap){
        super(invoker);
        this.key = key;
        this.exporterMap = exporterMap;
        exporterMap.put(key, this);
    }

    /**
     * 简单的不导出
     * 缓存中删除数据
     */
    public void unexport() {
        super.unexport();
        exporterMap.remove(key);
    }

}