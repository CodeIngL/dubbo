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
package com.alibaba.dubbo.remoting.exchange.support.header;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Client;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.ExchangeChannel;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.ExchangeHandler;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;

/**
 * DefaultMessageClient(默认的消息客户端，信件)
 *
 * @author william.liangf
 * @author chao.liuc
 */
public class HeaderExchangeClient implements ExchangeClient {

    private static final Logger logger = LoggerFactory.getLogger(HeaderExchangeClient.class);

    private static final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(2, new NamedThreadFactory("dubbo-remoting-client-heartbeat", true));

    // 心跳定时器
    private ScheduledFuture<?> heatbeatTimer;

    // 心跳超时，毫秒。缺省0，不会执行心跳。
    private int heartbeat;

    // 心跳超时时间
    private int heartbeatTimeout;

    // client对应dubbo对各种网络框架的客户端的实现
    private final Client client;

    // channel对应了dubbo自己对网络通信中channel中的封装
    private final ExchangeChannel channel;

    /**
     * 可交互的客户端
     * @param client 客户端
     */
    public HeaderExchangeClient(Client client) {
        if (client == null) {
            throw new IllegalArgumentException("client == null");
        }
        this.client = client;
        this.channel = new HeaderExchangeChannel(client);
        this.heartbeat = client.getUrl().getParameter(Constants.HEARTBEAT_KEY, 0);
        this.heartbeatTimeout = client.getUrl().getParameter(Constants.HEARTBEAT_TIMEOUT_KEY, heartbeat * 3);
        if (heartbeatTimeout < heartbeat * 2) {
            throw new IllegalStateException("heartbeatTimeout < heartbeatInterval * 2");
        }
        startHeatbeatTimer();
    }

    public ResponseFuture request(Object request) throws RemotingException {
        return channel.request(request);
    }

    public URL getUrl() {
        return channel.getUrl();
    }

    public InetSocketAddress getRemoteAddress() {
        return channel.getRemoteAddress();
    }

    public ResponseFuture request(Object request, int timeout) throws RemotingException {
        return channel.request(request, timeout);
    }

    public ChannelHandler getChannelHandler() {
        return channel.getChannelHandler();
    }

    public boolean isConnected() {
        return channel.isConnected();
    }

    public InetSocketAddress getLocalAddress() {
        return channel.getLocalAddress();
    }

    public ExchangeHandler getExchangeHandler() {
        return channel.getExchangeHandler();
    }

    public void send(Object message) throws RemotingException {
        channel.send(message);
    }

    /**
     * 网络客户端最外层的客户端操作
     * 接着---》特定的网络框架 nettyClient，minaClient。。
     * 此时的channel为HeaderExchangeChannel
     * @param message
     * @param sent    是否已发送完成
     * @throws RemotingException
     * @see HeaderExchangeChannel
     */
    public void send(Object message, boolean sent) throws RemotingException {
        channel.send(message, sent);
    }

    public boolean isClosed() {
        return channel.isClosed();
    }

    public void close() {
        doClose();
        channel.close();
    }

    public void close(int timeout) {
        doClose();
        channel.close(timeout);
    }

    public void reset(URL url) {
        client.reset(url);
    }

    @Deprecated
    public void reset(com.alibaba.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    public void reconnect() throws RemotingException {
        client.reconnect();
    }

    public Object getAttribute(String key) {
        return channel.getAttribute(key);
    }

    public void setAttribute(String key, Object value) {
        channel.setAttribute(key, value);
    }

    public void removeAttribute(String key) {
        channel.removeAttribute(key);
    }

    public boolean hasAttribute(String key) {
        return channel.hasAttribute(key);
    }

    private void startHeatbeatTimer() {
        stopHeartbeatTimer();
        if (heartbeat > 0) {
            heatbeatTimer = scheduled.scheduleWithFixedDelay(
                    new HeartBeatTask(new HeartBeatTask.ChannelProvider() {
                        public Collection<Channel> getChannels() {
                            return Collections.<Channel>singletonList(HeaderExchangeClient.this);
                        }
                    }, heartbeat, heartbeatTimeout),
                    heartbeat, heartbeat, TimeUnit.MILLISECONDS);
        }
    }

    private void stopHeartbeatTimer() {
        if (heatbeatTimer != null && !heatbeatTimer.isCancelled()) {
            try {
                heatbeatTimer.cancel(true);
                scheduled.purge();
            } catch (Throwable e) {
                if (logger.isWarnEnabled()) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
        heatbeatTimer = null;
    }

    private void doClose() {
        stopHeartbeatTimer();
    }

    @Override
    public String toString() {
        return "HeaderExchangeClient [channel=" + channel + "]";
    }
}