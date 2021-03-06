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
package com.alibaba.dubbo.rpc;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;

/**
 * RPC Invocation.
 *
 * @author qian.lei
 * @serial Don't change the class name and properties.
 */
public class RpcInvocation implements Invocation, Serializable {

    private static final long serialVersionUID = -4355285085441097045L;

    // 方法名
    private String methodName;

    // 方法参数类型数组
    private Class<?>[] parameterTypes;

    // 方法实际入参数组
    private Object[] arguments;

    // 附加信息
    private Map<String, String> attachments;

    // rpc调用者
    private transient Invoker<?> invoker;

    public RpcInvocation() {
    }

    /**
     * 构建一个RPC调用对象
     *
     * @param invocation 调用对象
     * @see #RpcInvocation(Invocation, Invoker)
     */
    public RpcInvocation(Invocation invocation) {
        this(invocation.getMethodName(), invocation.getParameterTypes(),
                invocation.getArguments(), invocation.getAttachments(), invocation.getInvoker());
    }

    /**
     * 构建一个RPC调用对象
     * 包装入参invocation，追加入参invoker的信息
     * @param invocation 调用对象
     * @param invoker    调用者，只利用其部分数据信息
     */
    public RpcInvocation(Invocation invocation, Invoker<?> invoker) {
        this(invocation.getMethodName(), invocation.getParameterTypes(),
                invocation.getArguments(), new HashMap<String, String>(invocation.getAttachments()), invocation.getInvoker());
        if (invoker != null) {
            URL url = invoker.getUrl();
            //path信息
            setAttachment(Constants.PATH_KEY, url.getPath());
            //interface信息
            if (url.hasParameter(Constants.INTERFACE_KEY)) {
                setAttachment(Constants.INTERFACE_KEY, url.getParameter(Constants.INTERFACE_KEY));
            }
            //group信息
            if (url.hasParameter(Constants.GROUP_KEY)) {
                setAttachment(Constants.GROUP_KEY, url.getParameter(Constants.GROUP_KEY));
            }
            //version信息
            if (url.hasParameter(Constants.VERSION_KEY)) {
                setAttachment(Constants.VERSION_KEY, url.getParameter(Constants.VERSION_KEY, "0.0.0"));
            }
            //timeout信息
            if (url.hasParameter(Constants.TIMEOUT_KEY)) {
                setAttachment(Constants.TIMEOUT_KEY, url.getParameter(Constants.TIMEOUT_KEY));
            }
            //token信息
            if (url.hasParameter(Constants.TOKEN_KEY)) {
                setAttachment(Constants.TOKEN_KEY, url.getParameter(Constants.TOKEN_KEY));
            }
            //application信息
            if (url.hasParameter(Constants.APPLICATION_KEY)) {
                setAttachment(Constants.APPLICATION_KEY, url.getParameter(Constants.APPLICATION_KEY));
            }
        }
    }

    /**
     * RPC的调用封装形式
     *
     * @param method    方法
     * @param arguments 方法参数
     * @see #RpcInvocation(String, Class[], Object[], Map, Invoker)
     */
    public RpcInvocation(Method method, Object[] arguments) {
        this(method.getName(), method.getParameterTypes(), arguments, null, null);
    }

    /**
     * RPC的调用封装形式
     *
     * @param method    方法
     * @param arguments 方法参数
     * @see #RpcInvocation(String, Class[], Object[], Map, Invoker)
     */
    public RpcInvocation(Method method, Object[] arguments, Map<String, String> attachment) {
        this(method.getName(), method.getParameterTypes(), arguments, attachment, null);
    }

    /**
     * RPC的调用封装形式
     *
     * @param methodName     方法名
     * @param parameterTypes 参数类型数组
     * @param arguments      参数值数组
     * @see #RpcInvocation(String, Class[], Object[], Map, Invoker)
     */
    public RpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] arguments) {
        this(methodName, parameterTypes, arguments, null, null);
    }

    /**
     * RPC的调用封装形式
     *
     * @param methodName     方法名
     * @param parameterTypes 参数类型数组
     * @param arguments      参数值数组
     * @param attachments    附加信息
     * @see #RpcInvocation(String, Class[], Object[], Map, Invoker)
     */
    public RpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] arguments, Map<String, String> attachments) {
        this(methodName, parameterTypes, arguments, attachments, null);
    }

    /**
     * RPC的调用封装形式（调用对象)
     *
     * @param methodName     方法名
     * @param parameterTypes 参数类型数组
     * @param arguments      参数值数组
     * @param attachments    附加信息
     * @param invoker        rpc执行者(调用者)
     */
    public RpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] arguments, Map<String, String> attachments, Invoker<?> invoker) {
        this.methodName = methodName;
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
        this.arguments = arguments == null ? new Object[0] : arguments;
        this.attachments = attachments == null ? new HashMap<String, String>() : attachments;
        this.invoker = invoker;
    }

    public Invoker<?> getInvoker() {
        return invoker;
    }

    public void setInvoker(Invoker<?> invoker) {
        this.invoker = invoker;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public Map<String, String> getAttachments() {
        return attachments;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
    }

    public void setArguments(Object[] arguments) {
        this.arguments = arguments == null ? new Object[0] : arguments;
    }

    public void setAttachments(Map<String, String> attachments) {
        this.attachments = attachments == null ? new HashMap<String, String>() : attachments;
    }

    public void setAttachment(String key, String value) {
        if (attachments == null) {
            attachments = new HashMap<String, String>();
        }
        attachments.put(key, value);
    }

    public void setAttachmentIfAbsent(String key, String value) {
        if (attachments == null) {
            attachments = new HashMap<String, String>();
        }
        if (!attachments.containsKey(key)) {
            attachments.put(key, value);
        }
    }

    public void addAttachments(Map<String, String> attachments) {
        if (attachments == null) {
            return;
        }
        if (this.attachments == null) {
            this.attachments = new HashMap<String, String>();
        }
        this.attachments.putAll(attachments);
    }

    public void addAttachmentsIfAbsent(Map<String, String> attachments) {
        if (attachments == null) {
            return;
        }
        for (Map.Entry<String, String> entry : attachments.entrySet()) {
            setAttachmentIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    public String getAttachment(String key) {
        if (attachments == null) {
            return null;
        }
        return attachments.get(key);
    }

    public String getAttachment(String key, String defaultValue) {
        if (attachments == null) {
            return defaultValue;
        }
        String value = attachments.get(key);
        if (value == null || value.length() == 0) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public String toString() {
        return "RpcInvocation [methodName=" + methodName + ", parameterTypes="
                + Arrays.toString(parameterTypes) + ", arguments=" + Arrays.toString(arguments)
                + ", attachments=" + attachments + "]";
    }

}