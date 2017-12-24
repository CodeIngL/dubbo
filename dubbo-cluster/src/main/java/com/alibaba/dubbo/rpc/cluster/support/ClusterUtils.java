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

import java.util.HashMap;
import java.util.Map;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;

/**
 * ClusterUtils
 * 
 * @author william.liangf
 */
public class ClusterUtils {

    /**
     * 合并参数工具类<br/>
     * 线程相关的不使用提供者的信息<br/>
     *
     * @param remoteUrl 远程url
     * @param localMap 待合并的参数信息
     * @return 合并后的元信息
     */
    public static URL mergeUrl(URL remoteUrl, Map<String, String> localMap) {
        Map<String, String> map = new HashMap<String, String>();

        Map<String, String> remoteMap = remoteUrl.getParameters();
        if (remoteMap != null && remoteMap.size() > 0) {
            map.putAll(remoteMap);
            //线程池配置不使用提供者的
            map.remove(Constants.THREAD_NAME_KEY);//threadname
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.THREAD_NAME_KEY);//default.threadname

            map.remove(Constants.THREADPOOL_KEY);//threadpool
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.THREADPOOL_KEY);//default.threadpool

            map.remove(Constants.CORE_THREADS_KEY);//corethreads
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.CORE_THREADS_KEY);//default.corethreads

            map.remove(Constants.THREADS_KEY);//threads
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.THREADS_KEY);//default.threads

            map.remove(Constants.QUEUES_KEY);//queues
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.QUEUES_KEY);//default.queues

            map.remove(Constants.ALIVE_KEY);//alive
            map.remove(Constants.DEFAULT_KEY_PREFIX + Constants.ALIVE_KEY);//default.alive
        }

        //本地覆盖远程的，如果键一样的话
        if (localMap != null && localMap.size() > 0) {
            map.putAll(localMap);
        }
        //相关的键值对重新使用远端的数据
        if (remoteMap != null && remoteMap.size() > 0) { 
            // dubbo这个键使用远端提供者的
            String dubbo = remoteMap.get(Constants.DUBBO_VERSION_KEY);
            if (dubbo != null && dubbo.length() > 0) {
                map.put(Constants.DUBBO_VERSION_KEY, dubbo);
            }
            // version这个键版使用远端提供者的
            String version = remoteMap.get(Constants.VERSION_KEY);
            if (version != null && version.length() > 0) {
                map.put(Constants.VERSION_KEY, version);
            }
            // group这个键使用远端提供者的
            String group = remoteMap.get(Constants.GROUP_KEY);
            if (group != null && group.length() > 0) {
                map.put(Constants.GROUP_KEY, group);
            }
            // methods这个键使用远端提供者的
            String methods = remoteMap.get(Constants.METHODS_KEY);
            if (methods != null && methods.length() > 0) {
                map.put(Constants.METHODS_KEY, methods);
            }
            // 合并filter，两者的合集
            String remoteFilter = remoteMap.get(Constants.REFERENCE_FILTER_KEY);
            String localFilter = localMap.get(Constants.REFERENCE_FILTER_KEY);
            if (remoteFilter != null && remoteFilter.length() > 0 && localFilter != null && localFilter.length() > 0) {
                localMap.put(Constants.REFERENCE_FILTER_KEY, remoteFilter + "," + localFilter);
            }
            // 合并listener，两者的合集
            String remoteListener = remoteMap.get(Constants.INVOKER_LISTENER_KEY);
            String localListener = localMap.get(Constants.INVOKER_LISTENER_KEY);
            if (remoteListener != null && remoteListener.length() > 0 && localListener != null && localListener.length() > 0) {
                localMap.put(Constants.INVOKER_LISTENER_KEY, remoteListener + "," + localListener);
            }
        }
        //构建新的url
        return remoteUrl.clearParameters().addParameters(map);
    }

    private ClusterUtils() {}
    
}