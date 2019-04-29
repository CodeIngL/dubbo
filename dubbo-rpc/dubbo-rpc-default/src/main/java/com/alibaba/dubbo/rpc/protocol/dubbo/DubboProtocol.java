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
package com.alibaba.dubbo.rpc.protocol.dubbo;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.Transporter;
import com.alibaba.dubbo.remoting.exchange.ExchangeChannel;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.ExchangeHandler;
import com.alibaba.dubbo.remoting.exchange.ExchangeServer;
import com.alibaba.dubbo.remoting.exchange.Exchangers;
import com.alibaba.dubbo.remoting.exchange.support.ExchangeHandlerAdapter;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.protocol.AbstractProtocol;

import static com.alibaba.dubbo.common.Constants.*;
import static com.alibaba.dubbo.common.extension.ExtensionLoader.getExtensionLoader;
import static com.alibaba.dubbo.common.utils.StringUtils.isEmpty;
import static com.alibaba.dubbo.common.utils.StringUtils.isNotEmpty;

/**
 * dubbo protocol support.
 *
 * @author qian.lei
 * @author william.liangf
 * @author chao.liuc
 */
public class DubboProtocol extends AbstractProtocol {

    public static final String NAME = "dubbo";

    public static final int DEFAULT_PORT = 20880;

    private final Map<String, ExchangeServer> serverMap = new ConcurrentHashMap<String, ExchangeServer>(); // <host:port,Exchanger>

    private final Map<String, ReferenceCountExchangeClient> referenceClientMap = new ConcurrentHashMap<String, ReferenceCountExchangeClient>(); // <host:port,Exchanger>

    private final ConcurrentMap<String, LazyConnectExchangeClient> ghostClientMap = new ConcurrentHashMap<String, LazyConnectExchangeClient>();

    //consumer side export a stub service for dispatching event
    //servicekey-stubmethods
    private final ConcurrentMap<String, String> stubServiceMethodsMap = new ConcurrentHashMap<String, String>();

    private static final String IS_CALLBACK_SERVICE_INVOKE = "_isCallBackServiceInvoke";

    /**
     * 最核心的处理器，众多处理器，最后应用的处理器
     */
    private ExchangeHandler requestHandler = new ExchangeHandlerAdapter() {
        @Override
        public Object reply(ExchangeChannel channel, Object message) throws RemotingException {
            if (message instanceof Invocation) {
                Invocation inv = (Invocation) message;
                Invoker<?> invoker = getInvoker(channel, inv);
                if (Boolean.TRUE.toString().equals(inv.getAttachments().get(IS_CALLBACK_SERVICE_INVOKE))) {//如果是callback 需要处理高版本调用低版本的问题
                    String methodsStr = invoker.getUrl().getParameters().get("methods");
                    boolean hasMethod = false;
                    if (methodsStr == null || methodsStr.indexOf(",") == -1) {
                        hasMethod = inv.getMethodName().equals(methodsStr);
                    } else {
                        for (String method : methodsStr.split(",")) {
                            if (inv.getMethodName().equals(method)) {
                                hasMethod = true;
                                break;
                            }
                        }
                    }
                    if (!hasMethod) {
                        logger.warn(new IllegalStateException("The methodName " + inv.getMethodName()
                                + " not found in callback service interface ,invoke will be ignored. please update the api interface. url is:" + invoker.getUrl()) + " ,invocation is :" + inv);
                        return null;
                    }
                }
                RpcContext.getContext().setRemoteAddress(channel.getRemoteAddress());
                return invoker.invoke(inv);
            }
            throw new RemotingException(channel, "Unsupported request: " + message == null ? null : (message.getClass().getName()
                    + ": " + message) + ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress());
        }

        @Override
        public void received(Channel channel, Object message) throws RemotingException {
            if (message instanceof Invocation) {
                reply((ExchangeChannel) channel, message);
            } else {
                super.received(channel, message);
            }
        }

        @Override
        public void connected(Channel channel) throws RemotingException {
            invoke(channel, Constants.ON_CONNECT_KEY);
        }

        @Override
        public void disconnected(Channel channel) throws RemotingException {
            if (logger.isInfoEnabled()) {
                logger.info("disconected from " + channel.getRemoteAddress() + ",url:" + channel.getUrl());
            }
            invoke(channel, Constants.ON_DISCONNECT_KEY);
        }

        private void invoke(Channel channel, String methodKey) {
            Invocation invocation = createInvocation(channel, channel.getUrl(), methodKey);
            if (invocation != null) {
                try {
                    received(channel, invocation);
                } catch (Throwable t) {
                    logger.warn("Failed to invoke event method " + invocation.getMethodName() + "(), cause: " + t.getMessage(), t);
                }
            }
        }

        private Invocation createInvocation(Channel channel, URL url, String methodKey) {
            String method = url.getParameter(methodKey);
            if (method == null || method.length() == 0) {
                return null;
            }
            RpcInvocation invocation = new RpcInvocation(method, new Class<?>[0], new Object[0]);
            invocation.setAttachment(PATH_KEY, url.getPath());
            invocation.setAttachment(GROUP_KEY, url.getParameter(GROUP_KEY));
            invocation.setAttachment(INTERFACE_KEY, url.getParameter(INTERFACE_KEY));
            invocation.setAttachment(VERSION_KEY, url.getParameter(VERSION_KEY));
            if (url.getParameter(STUB_EVENT_KEY, false)) {
                invocation.setAttachment(STUB_EVENT_KEY, Boolean.TRUE.toString());
            }
            return invocation;
        }
    };

    private static DubboProtocol INSTANCE;

    public DubboProtocol() {
        INSTANCE = this;
    }

    public static DubboProtocol getDubboProtocol() {
        if (INSTANCE == null) {
            getExtensionLoader(Protocol.class).getExtension(DubboProtocol.NAME); // load
        }
        return INSTANCE;
    }

    public Collection<ExchangeServer> getServers() {
        return Collections.unmodifiableCollection(serverMap.values());
    }

    public Collection<Exporter<?>> getExporters() {
        return Collections.unmodifiableCollection(exporterMap.values());
    }

    Map<String, Exporter<?>> getExporterMap() {
        return exporterMap;
    }

    private boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(channel.getUrl().getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    Invoker<?> getInvoker(Channel channel, Invocation inv) throws RemotingException {
        int port = channel.getLocalAddress().getPort();
        Map<String,String> attachments= inv.getAttachments();
        String path = attachments.get(PATH_KEY);
        boolean isStubServiceInvoke = Boolean.TRUE.toString().equals(attachments.get(STUB_EVENT_KEY)); //如果是客户端的回调服务.
        if (isStubServiceInvoke) {
            port = channel.getRemoteAddress().getPort();
        }
        boolean isCallBackServiceInvoke = isClientSide(channel) && !isStubServiceInvoke;//callback
        if (isCallBackServiceInvoke) {
            path = attachments.get(PATH_KEY) + "." + attachments.get(CALLBACK_SERVICE_KEY);
            attachments.put(IS_CALLBACK_SERVICE_INVOKE, Boolean.TRUE.toString());
        }
        String serviceKey = serviceKey(port, path, attachments.get(VERSION_KEY), attachments.get(GROUP_KEY));

        DubboExporter<?> exporter = (DubboExporter<?>) exporterMap.get(serviceKey);

        if (exporter == null)
            throw new RemotingException(channel, "Not found exported service: " + serviceKey + " in " + exporterMap.keySet() + ", may be version or group mismatch " + ", channel: consumer: " + channel.getRemoteAddress() + " --> provider: " + channel.getLocalAddress() + ", message:" + inv);

        return exporter.getInvoker();
    }

    public Collection<Invoker<?>> getInvokers() {
        return Collections.unmodifiableCollection(invokers);
    }

    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    //======= 服务端相关 =======

    /**
     * <p>
     * dubbo默认的导出方式
     * <ul>
     * <li>通过invoker获得协议配置的url元信息</li><br/>
     * <li>获得服务的标识，通过url元信息构建（${Group}+"/"+${Name}+":"${Version}+":"${port}）</li><br/>
     * <li>构建导出对象，构建键值对(服务标识，服务导出队形)加入缓存</li><br/>
     * <li>根据URL元信息,对配置有桩(dubbo.stub.event:true)的且不是回调作用(is_callback_service:false)的服务，将其桩方法（键为dubbo.stub.event.methods）加入缓存中</li><br/>
     * </ul>
     * </p>
     *
     * @param invoker 服务的执行体
     * @param <T>
     * @return
     * @throws RpcException
     * @see #serviceKey(URL) （serviceGroup+"/"+serviceName+":"serviceVersion+":"port）
     * @see URL#getServiceKey() （serviceGroup+"/"+serviceName+":"serviceVersion）
     */
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {

        URL url = invoker.getUrl();//获得协议相关的url

        String key = serviceKey(url);//从url中获得key服务标识

        DubboExporter<T> exporter = new DubboExporter<T>(invoker, key, exporterMap);//构建dubbo协议下的的exporter

        exporterMap.put(key, exporter);//放入缓存

        Boolean isStubSupportEvent = url.getParameter(STUB_EVENT_KEY, DEFAULT_STUB_EVENT); //从url中获得dubbo.stub.event的值 默认是false
        Boolean isCallbackService = url.getParameter(IS_CALLBACK_SERVICE, false);//从url中获得is_callback_service的值 默认是false
        if (isStubSupportEvent && !isCallbackService) {
            String stubServiceMethods = url.getParameter(Constants.STUB_EVENT_METHODS_KEY); //获得桩服务名字
            if (isEmpty(stubServiceMethods)) {
                if (logger.isWarnEnabled()) {
                    logger.warn(new IllegalStateException("consumer [" + url.getParameter(INTERFACE_KEY) +
                            "], has set stubproxy support event ,but no stub methods founded."));
                }
            } else {
                stubServiceMethodsMap.put(url.getServiceKey(), stubServiceMethods);//存在放入缓存
            }
        }
        openServer(url);//open
        return exporter;
    }

    /**
     * 启动网络服务
     * <ul>
     * <li>从url元信息中获取地址：端口存在的情况下是host+port，否则是host</li><br/>
     * <li>尝试从元信息中获得键值对，键为“isserver”，没有默认为true</li><br/>
     * <li>尝试从缓存中获得服务对象，无则新建,{@link #createServer(URL)}</li><br/>
     * </ul>
     *
     * @param url 元信息
     */
    private void openServer(URL url) {
        String key = url.getAddress();//获得地址
        boolean isServer = url.getParameter(IS_SERVER_KEY, true);//client 也可以暴露一个只有server可以调用的服务。//获得url中键为isserver的值，默认是true
        if (isServer) {
            ExchangeServer server = serverMap.get(key);//缓存
            if (server == null) {
                serverMap.put(key, createServer(url));
            } else {
                server.reset(url);//server支持reset,配合override功能使用
            }
        }
    }

    /**
     * 创建网络服务对象，改造url，补充一些元信息
     * <ul>
     * <li>增加键值对(channel.readonly.sent:true),键存在不做任何操作</li>
     * <li>增加键值对(heartbeat:60000),键存在不做任何操作</li>
     * <li>添加键值对(codec:dubbo),用来确定特定的编码解码器，默认是DubboCodec其名字为dubbo</li>
     * <li>获得网络框架类型通过键("server"),默认是netty</li>
     * <li>绑定url和交换源处理器得到服务对象</li>
     * </ul>
     *
     * @param url 元信息
     * @return
     */
    private ExchangeServer createServer(URL url) {
        url = url.addParameterIfAbsent(CHANNEL_READONLYEVENT_SENT_KEY, Boolean.TRUE.toString());//默认开启server关闭时发送readonly事件
        url = url.addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT));//默认开启heartbeat

        String str = url.getParameter(SERVER_KEY, DEFAULT_REMOTING_SERVER);//从url中获得服务类型，默认是netty
        if (isNotEmpty(str) && !getExtensionLoader(Transporter.class).hasExtension(str))
            throw new RpcException("Unsupported server type: " + str + ", url: " + url);

        url = url.addParameter(CODEC_KEY, DubboCodec.NAME);

        ExchangeServer server;
        try {
            server = Exchangers.bind(url, requestHandler); //开启服务
        } catch (RemotingException e) {
            throw new RpcException("Fail to start server(url: " + url + ") " + e.getMessage(), e);
        }

        //检测类型匹配
        str = url.getParameter(Constants.CLIENT_KEY); //获得url中key为client的值
        if (isNotEmpty(str)) {
            Set<String> supportedTypes = getExtensionLoader(Transporter.class).getSupportedExtensions();
            if (!supportedTypes.contains(str)) {
                throw new RpcException("Unsupported client type: " + str);
            }
        }
        return server;
    }


    //======= 消费端connect相关 =======

    /**
     * 消费端引用获得相应的invoker
     *
     * @param serviceType 接口类型
     * @param url         元信息
     * @param <T>         接口类型
     * @return DubboInvoker
     * @throws RpcException 异常信息
     */
    public <T> Invoker<T> refer(Class<T> serviceType, URL url) throws RpcException {
        DubboInvoker<T> invoker = new DubboInvoker<T>(serviceType, url, getClients(url), invokers);
        invokers.add(invoker);
        return invoker;
    }

    /**
     * 获得客户端
     *
     * @param url 元信息
     * @return 客户端实例
     */
    private ExchangeClient[] getClients(URL url) {
        //是否共享连接
        boolean service_share_connect = false;
        int connections = url.getParameter(Constants.CONNECTIONS_KEY, 0);
        // 如果connections不配置，则共享连接，否则每服务每连接
        // 可能存在瓶颈，TCP的网络状态
        if (connections == 0) {
            service_share_connect = true;
            connections = 1;
        }
        //构建网络客户端
        ExchangeClient[] clients = new ExchangeClient[connections];
        for (int i = 0; i < clients.length; i++) {
            if (service_share_connect) {
                clients[i] = getSharedClient(url);
            } else {
                clients[i] = initClient(url);
            }
        }
        return clients;
    }

    /**
     * 获取共享连接
     * <ul>尝试操作缓存结构{@link #referenceClientMap}获得客户端实例
     * <li>构建键:url的地址信息{@link URL#getAddress()}</li><br/>
     * <li>尝试在缓存映射中获得客户端</li><br/>
     * <li>检验客户端连接的有效性</li><br/>
     * </ul>
     *
     * @param url 元信息
     * @return 客户端
     */
    private ExchangeClient getSharedClient(URL url) {
        String key = url.getAddress();
        ReferenceCountExchangeClient client = referenceClientMap.get(key);
        if (client != null) {
            if (!client.isClosed()) {
                client.incrementAndGetCount();
                return client;
            } else {
                referenceClientMap.remove(key);
            }
        }
        client = new ReferenceCountExchangeClient(initClient(url), ghostClientMap);
        referenceClientMap.put(key, client);
        ghostClientMap.remove(key);
        return client;
    }

    /**
     * 创建新连接.
     * <ul>
     * <li>为url添加解码编码器</li>
     * <li>为url添加心跳的信息</li>
     * <li>选择合适的网络通信框架默认是netty</li>
     * <li>构建直连的连接还是稍后暴露的连接</li>
     * </ul>
     * 延迟连接下返回LazyConnectExchangeClient
     * 非延迟连接下返回HeaderExchangeClient
     *
     * @param url 元信息
     * @return 客户端实例
     */
    private ExchangeClient initClient(URL url) {
        url = url.addParameter(CODEC_KEY, DubboCodec.NAME);
        url = url.addParameterIfAbsent(HEARTBEAT_KEY, String.valueOf(DEFAULT_HEARTBEAT));

        // BIO存在严重性能问题，暂时不允许使用
        String str = url.getParameter(Constants.CLIENT_KEY, url.getParameter(SERVER_KEY, Constants.DEFAULT_REMOTING_CLIENT));
        if (!getExtensionLoader(Transporter.class).hasExtension(str)) {
            throw new RpcException("Unsupported client type: " + str + "," +
                    " supported client type is " + StringUtils.join(getExtensionLoader(Transporter.class).getSupportedExtensions(), " "));
        }

        ExchangeClient client;
        try {
            //设置连接应该是lazy的 
            if (url.getParameter(Constants.LAZY_CONNECT_KEY, false)) {
                client = new LazyConnectExchangeClient(url, requestHandler);
            } else {
                client = Exchangers.connect(url, requestHandler);
            }
        } catch (RemotingException e) {
            throw new RpcException("Fail to create remoting client for service(" + url + "): " + e.getMessage(), e);
        }
        return client;
    }

    public void destroy() {
        for (String key : new ArrayList<String>(serverMap.keySet())) {
            ExchangeServer server = serverMap.remove(key);
            if (server != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close dubbo server: " + server.getLocalAddress());
                    }
                    server.close(getServerShutdownTimeout());
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }

        for (String key : new ArrayList<String>(referenceClientMap.keySet())) {
            ExchangeClient client = referenceClientMap.remove(key);
            if (client != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close dubbo connect: " + client.getLocalAddress() + "-->" + client.getRemoteAddress());
                    }
                    client.close();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }

        for (String key : new ArrayList<String>(ghostClientMap.keySet())) {
            ExchangeClient client = ghostClientMap.remove(key);
            if (client != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close dubbo connect: " + client.getLocalAddress() + "-->" + client.getRemoteAddress());
                    }
                    client.close();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }
        stubServiceMethodsMap.clear();
        super.destroy();
    }
}