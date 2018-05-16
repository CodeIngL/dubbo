/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting.transport.netty4;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NettyClientHandler
 */
@io.netty.channel.ChannelHandler.Sharable
public class NettyServerHandler extends NettyHandler {

    private final Map<String, Channel> channels = new ConcurrentHashMap<String, Channel>(); // <ip:port, channel>

    public NettyServerHandler(URL url, ChannelHandler handler) {
        super(url, handler);
    }

    public Map<String, Channel> getChannels() {
        return channels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelActive();
        NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), getUrl(), getHandler());
        try {
            if (channel != null) {
                channels.put(NetUtils.toAddressString((InetSocketAddress) ctx.channel().remoteAddress()), channel);
            }
            getHandler().connected(channel);
        } finally {
            NettyChannel.removeChannelIfDisconnected(ctx.channel());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), getUrl(), getHandler());
        try {
            channels.remove(NetUtils.toAddressString((InetSocketAddress) ctx.channel().remoteAddress()));
            getHandler().disconnected(channel);
        } finally {
            NettyChannel.removeChannelIfDisconnected(ctx.channel());
        }
    }


    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise future)
            throws Exception {

    }

}