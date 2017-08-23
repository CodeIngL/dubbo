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
package com.alibaba.dubbo.registry.support;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.alibaba.dubbo.registry.RegistryService;

/**
 * AbstractRegistryFactory. (SPI, Singleton, ThreadSafe)
 *
 * @author william.liangf
 * @see com.alibaba.dubbo.registry.RegistryFactory
 */
public abstract class AbstractRegistryFactory implements RegistryFactory {

    // 日志输出
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRegistryFactory.class);

    // 注册中心获取过程锁
    private static final ReentrantLock LOCK = new ReentrantLock();

    // 注册中心集合 Map<String, Registry>
    // key为相应的注册中心url的部分:protocol://username:password@ip:port/group/com.alibaba.dubbo.registry:version,value为对应的注册中心
    private static final Map<String, Registry> REGISTRIES = new ConcurrentHashMap<String, Registry>();

    /**
     * 获取所有注册中心
     *
     * @return 所有注册中心
     */
    public static Collection<Registry> getRegistries() {
        return Collections.unmodifiableCollection(REGISTRIES.values());
    }

    /**
     * 关闭所有已创建注册中心
     */
    public static void destroyAll() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Close all registries " + getRegistries());
        }
        // 锁定注册中心关闭过程
        LOCK.lock();
        try {
            for (Registry registry : getRegistries()) {
                try {
                    registry.destroy();
                } catch (Throwable e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            REGISTRIES.clear();
        } finally {
            // 释放锁
            LOCK.unlock();
        }
    }

    /**
     * 获得实际的注册中心实例
     * <p>相关逻辑
     * <ul>对url进行设置，path为com.alibaba.dubbo.registry.Registry,移除其他非注册中心的属性，比如export，refer</ul><br/>
     * <ul>从url中获得关键key:protocol://username:password@ip:port/group/com.alibaba.dubbo.registry.{@link RegistryService}:version，一个注册中心对应的url对应一个注册中心</ul><br/>
     * <ul>缓存操作，构建或获取注册中心实际实例</ul><br/>
     * </p>
     * tip: 在一般情况下，对应某个特定的protocol，其userName和password都对应了，对本机来说，ip和port也就定了，因此只有group是可以操作的部分，也就是说相关的group组成了一个注册中心，而且這個group是
     * 有注册中心url自己配置的而不是从普通的url上转移过来的，也就是说一般的开发者不会配置该项group，对于zookeeper来说，一般开发者也不会配置username，password，和version，所以最终的结果是
     * zookeeper://ip:port/com.alibaba.dubbo.registry.{@link RegistryService}
     *
     * @param url 注册中心地址，不允许为空
     * @return 注册中心实例
     */
    public Registry getRegistry(URL url) {
        //写入path，增加键值对，移除会变化的部分，主要是export键和refer键
        url = url.setPath(RegistryService.class.getName())
                .addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName())
                .removeParameters(Constants.EXPORT_KEY, Constants.REFER_KEY);
        //获得注册中心和该注册中心url对应的key值:protocol://username:password@ip:port/group/com.alibaba.dubbo.registry:version
        String key = url.toServiceString();
        //锁定注册中心获取过程，保证注册中心单一实例
        //尝试从缓存中取，没有则新建
        LOCK.lock();
        try {
            Registry registry = REGISTRIES.get(key);
            if (registry != null) {
                return registry;
            }
            //创建注册中心实例，
            registry = createRegistry(url);
            if (registry == null) {
                throw new IllegalStateException("Can not create registry " + url);
            }
            REGISTRIES.put(key, registry);
            return registry;
        } finally {
            LOCK.unlock();
        }
    }

    protected abstract Registry createRegistry(URL url);

}