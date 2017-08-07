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
package com.alibaba.dubbo.common.bytecode;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.dubbo.common.utils.ClassHelper;
import com.alibaba.dubbo.common.utils.ReflectUtils;

/**
 * Proxy.
 *
 * @author qian.lei
 */

public abstract class Proxy {

    //代理的数量
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);

    //
    private static final String PACKAGE_NAME = Proxy.class.getPackage().getName();

    //空的invoker
    public static final InvocationHandler RETURN_NULL_INVOKER = new InvocationHandler() {
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    };

    //不支持的invoker
    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };

    //缓存结构(类加载器:Map)
    private static final Map<ClassLoader, Map<String, Object>> ProxyCacheMap = new WeakHashMap<ClassLoader, Map<String, Object>>();

    //中间态，用于代表正在进行缓存操作
    private static final Object PendingGenerationMarker = new Object();

    /**
     * 获得代理
     *
     * @param ics 接口数组
     * @return 代理实例
     * @see #getProxy(ClassLoader, Class[])
     */
    public static Proxy getProxy(Class<?>... ics) {
        return getProxy(ClassHelper.getCallerClassLoader(Proxy.class), ics);
    }

    /**
     * 获得代理
     * <p>
     * <ul>
     * <li>检查接口数组，java规范规定</li>
     * <li>连接接口数组[com.codeL.A,com.codeL.B]-->com.codeL.A;com.codeL.B为key</li>
     * <li>获得入参类加载器对应的缓存，无则新建</li>
     * <li>尝试从缓存中使用key获得相应的代理</li>
     * <li>尝试从缓存中使用key获得相应的代理</li>
     * <li>见代码注释</li>
     * </ul>
     *
     * @param cl  类加载器
     * @param ics 接口数组
     * @return 代理实例
     */
    private static Proxy getProxy(ClassLoader cl, Class<?>... ics) {
        if (ics.length > 65535)
            throw new IllegalArgumentException("interface limit exceeded");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ics.length; i++) {
            String itf = ics[i].getName();
            if (!ics[i].isInterface())
                throw new RuntimeException(itf + " is not a interface.");

            Class<?> tmp = null;
            try {
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            if (tmp != ics[i])
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");

            sb.append(itf).append(';');
        }

        // use interface class name list as key.
        String key = sb.toString();

        // get cache by class loader.
        Map<String, Object> cache;
        synchronized (ProxyCacheMap) {
            cache = ProxyCacheMap.get(cl);
            if (cache == null) {
                cache = new HashMap<String, Object>();
                ProxyCacheMap.put(cl, cache);
            }
        }

        Proxy proxy = null;
        synchronized (cache) {
            do {
                Object value = cache.get(key);
                if (value instanceof Reference<?>) {
                    proxy = (Proxy) ((Reference<?>) value).get();
                    if (proxy != null)
                        return proxy;
                }

                if (value == PendingGenerationMarker) {
                    try {
                        cache.wait();
                    } catch (InterruptedException e) {
                    }
                } else {
                    cache.put(key, PendingGenerationMarker);
                    break;
                }
            }
            while (true);
        }

        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;
        ClassGenerator ccp = null, ccm = null;
        try {
            ccp = ClassGenerator.newInstance(cl);

            Set<String> worked = new HashSet<String>();
            List<Method> methods = new ArrayList<Method>();

            //检验接口的可访问性
            for (int i = 0; i < ics.length; i++) {
                if (!Modifier.isPublic(ics[i].getModifiers())) {
                    String npkg = ics[i].getPackage().getName();
                    if (pkg == null) {
                        pkg = npkg;
                    } else {
                        if (!pkg.equals(npkg))
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                    }
                }
                ccp.addInterface(ics[i]);

                //添加方法描述到worked中
                for (Method method : ics[i].getMethods()) {
                    String desc = ReflectUtils.getDesc(method);
                    if (worked.contains(desc)) {
                        continue;
                    }
                    worked.add(desc);

                    int ix = methods.size();
                    Class<?> rt = method.getReturnType();
                    Class<?>[] pts = method.getParameterTypes();

                    //构建方法的入参
                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    for (int j = 0; j < pts.length; j++) {
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    }
                    //委托给handler调用
                    code.append(" Object ret = handler.invoke(this, methods[" + ix + "], args);");
                    //返回参数不是void的处理
                    if (!Void.TYPE.equals(rt)) {
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");
                    }
                    methods.add(method);
                    ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                }
            }

            if (pkg == null) {
                pkg = PACKAGE_NAME;
            }

            //创建ProxyInstance类
            String pcn = pkg + ".proxy" + id;
            ccp.setClassName(pcn);
            //字段 Method[]methods
            ccp.addField("public static java.lang.reflect.Method[] methods;");
            //字段 InvocationHandler handler
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");
            //构造函数完成handler的传入
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");
            //默认构造函数
            ccp.addDefaultConstructor();
            Class<?> clazz = ccp.toClass();
            //设置默认值methods
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));

            //创建Proxy类
            String fcn = Proxy.class.getName() + id;
            ccm = ClassGenerator.newInstance(cl);
            ccm.setClassName(fcn);
            //添加默认构造函数
            ccm.addDefaultConstructor();
            //添加父类
            ccm.setSuperClass(Proxy.class);
            //添加方法newInstance(InvocationHandler h) 获得一个新建 ProxyInstance类对象
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn + "($1); }");
            Class<?> pc = ccm.toClass();
            proxy = (Proxy) pc.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // release ClassGenerator
            if (ccp != null) {
                ccp.release();
            }
            if (ccm != null) {
                ccm.release();
            }
            synchronized (cache) {
                if (proxy == null) {
                    cache.remove(key);
                } else {
                    //放入特定的应用，解锁PendingGenerationMarker这个过程
                    cache.put(key, new WeakReference<Proxy>(proxy));
                }
                cache.notifyAll();
            }
        }
        return proxy;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);

    protected Proxy() {
    }

    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl)
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            if (Byte.TYPE == cl)
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            if (Character.TYPE == cl)
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            if (Double.TYPE == cl)
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            if (Float.TYPE == cl)
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            if (Integer.TYPE == cl)
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            if (Long.TYPE == cl)
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            if (Short.TYPE == cl)
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }
}