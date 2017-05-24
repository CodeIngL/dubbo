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

import java.util.ArrayList;
import java.util.List;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.compiler.support.AdaptiveCompiler;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.config.support.Parameter;


/**
 * ApplicationConfig
 * 
 * @author william.liangf
 * @export
 */
public class ApplicationConfig extends AbstractConfig {

    private static final long    serialVersionUID = 5508512956753757169L;

    // 应用名称,必填
    //当前应用名称，用于注册中心计算应用间依赖关系，
    // 注意：消费者和提供者应用名不要一样，此参数不是匹配条件，你当前项目叫什么名字就填什么，和提供者消费者角色无关，
    // 比如：kylin应用调用了morgan应用的服务，则kylin项目配成kylin，morgan项目配成morgan，可能kylin也提供其它服务给别人使用，但kylin项目永远配成kylin，这样注册中心将显示kylin依赖于morgan
    private String               name;

    // 模块版本
    // 当前应用的版本
    private String               version;

    // 应用负责人
    // 应用负责人，用于服务治理，请填写负责人公司邮箱前缀
    private String               owner;

    // 组织名(BU或部门)
    //组织名称(BU或部门)，用于注册中心区分服务来源，此配置项建议不要使用autoconfig，直接写死在配置中，比如china,intl,itu,crm,asc,dw,aliexpress等
    private String               organization;

    // 分层
    // 用于服务分层对应的架构。如，intl、china。不同的架构使用不同的分层
    private String               architecture;

    // 环境，如：dev/test/run
    //应用环境，如：develop/test/product，不同环境使用不同的缺省值，以及作为只用于开发测试功能的限制条件
    private String               environment;

    // Java代码编译器
    //Java字节码编译器，用于动态类的生成，可选：jdk或javassist
    private String               compiler;

    // 日志输出方式
    //日志输出方式，可选：slf4j,jcl,log4j,jdk
    private String               logger;

    // 注册中心
    private List<RegistryConfig> registries;

    // 服务监控
    private MonitorConfig        monitor;

    // 是否为缺省
    private Boolean              isDefault;

    public ApplicationConfig() {
    }
    
    public ApplicationConfig(String name) {
        setName(name);
    }
    
    @Parameter(key = Constants.APPLICATION_KEY, required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        checkName("name", name);
        this.name = name;
        if (id == null || id.length() == 0) {
            id = name;
        }
    }

    @Parameter(key = "application.version")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        checkMultiName("owner", owner);
        this.owner = owner;
    }

	public String getOrganization() {
		return organization;
	}

	public void setOrganization(String organization) {
	    checkName("organization", organization);
		this.organization = organization;
	}

	public String getArchitecture() {
        return architecture;
    }

    public void setArchitecture(String architecture) {
        checkName("architecture", architecture);
        this.architecture = architecture;
    }

    public String getEnvironment() {
		return environment;
	}

	public void setEnvironment(String environment) {
	    checkName("environment", environment);
	    if(environment != null) {
            if (! ("develop".equals(environment) || "test".equals(environment) || "product".equals(environment))) {
                throw new IllegalStateException("Unsupported environment: " + environment + ", only support develop/test/product, default is product.");
            }
        }
		this.environment = environment;
	}

    public RegistryConfig getRegistry() {
        return registries == null || registries.size() == 0 ? null : registries.get(0);
    }

    public void setRegistry(RegistryConfig registry) {
        List<RegistryConfig> registries = new ArrayList<RegistryConfig>(1);
        registries.add(registry);
        this.registries = registries;
    }

    public List<RegistryConfig> getRegistries() {
        return registries;
    }

    @SuppressWarnings({ "unchecked" })
    public void setRegistries(List<? extends RegistryConfig> registries) {
        this.registries = (List<RegistryConfig>)registries;
    }

    public MonitorConfig getMonitor() {
        return monitor;
    }

    public void setMonitor(MonitorConfig monitor) {
        this.monitor = monitor;
    }

    public void setMonitor(String monitor) {
        this.monitor = new MonitorConfig(monitor);
    }

    public String getCompiler() {
        return compiler;
    }

    public void setCompiler(String compiler) {
        this.compiler = compiler;
        AdaptiveCompiler.setDefaultCompiler(compiler);
    }

    public String getLogger() {
        return logger;
    }

    public void setLogger(String logger) {
        this.logger = logger;
        LoggerFactory.setLoggerAdapter(logger);
    }

    public Boolean isDefault() {
        return isDefault;
    }

    public void setDefault(Boolean isDefault) {
        this.isDefault = isDefault;
    }

}