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
package com.alibaba.dubbo.rpc.cluster.support;

import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Directory;

/**
 * 当一个接口引用有多个组的时候，采用该集群策略进行调用
 * @see MergeableClusterInvoker
 * @author <a href="mailto:gang.lvg@alibaba-inc.com">kimi</a>
 */
public class MergeableCluster implements Cluster {

    public static final String NAME = "mergeable";

    /**
     * 合并的集群方式调用
     * @param directory 目录服务
     * @param <T>
     * @return 合并的集群方式调用者对象
     * @throws RpcException rpc异常
     */
    public <T> Invoker<T> join( Directory<T> directory ) throws RpcException {
        return new MergeableClusterInvoker<T>( directory );
    }

}
