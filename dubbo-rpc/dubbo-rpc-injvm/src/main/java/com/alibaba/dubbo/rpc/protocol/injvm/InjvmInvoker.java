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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.protocol.AbstractInvoker;

/**
 * InjvmInvoker
 * 
 * @author william.liangf
 */
class InjvmInvoker<T> extends AbstractInvoker<T> {

    private final String key;

    private final Map<String, Exporter<?>> exporterMap;

    /**
     * 内部injvm的invoker实现
     * @param type 类类型
     * @param url 元信息
     * @param key 关键键
     * @param exporterMap 缓存
     */
    InjvmInvoker(Class<T> type, URL url, String key, Map<String, Exporter<?>> exporterMap){
        super(type, url);
        this.key = key;
        this.exporterMap = exporterMap;
    }

    /**
     * 在服务有暴露存在的情况下，使用服务的状态
     * 否则使用父类的方法来确定
     * @return 可用性
     */
    @Override
	public boolean isAvailable() {
    	InjvmExporter<?> exporter = (InjvmExporter<?>) exporterMap.get(key);
    	if (exporter == null)  {
            return false;
        } else {
        	return super.isAvailable();
        }
	}

    /**
     * injvm的doInvoker的实现，
     * 获得暴露的服务，然后直接使用该export进行调用
     * @param invocation 调用对象
     * @return 结果
     * @throws Throwable 异常
     */
	public Result doInvoke(Invocation invocation) throws Throwable {
        Exporter<?> exporter = InjvmProtocol.getExporter(exporterMap, getUrl());
        if (exporter == null)  {
            throw new RpcException("Service [" + key + "] not found.");
        }
        RpcContext.getContext().setRemoteAddress(NetUtils.LOCALHOST, 0);
        return exporter.getInvoker().invoke(invocation);
    }
}