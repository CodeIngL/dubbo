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
package com.alibaba.dubbo.remoting.transport.netty;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.buffer.DynamicChannelBuffer;

/**
 * NettyCodecAdapter.
 * netty编码解码器适配
 * 
 * @author qian.lei
 */
final class NettyCodecAdapter {

    private final ChannelHandler encoder = new InternalEncoder();
    
    private final ChannelHandler decoder = new InternalDecoder();

    private final Codec2         codec;
    
    private final URL            url;
    
    private final int            bufferSize;
    
    private final com.alibaba.dubbo.remoting.ChannelHandler handler;

    public NettyCodecAdapter(Codec2 codec, URL url, com.alibaba.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
        int b = url.getPositiveParameter(Constants.BUFFER_KEY, Constants.DEFAULT_BUFFER_SIZE);
        this.bufferSize = b >= Constants.MIN_BUFFER_SIZE && b <= Constants.MAX_BUFFER_SIZE ? b : Constants.DEFAULT_BUFFER_SIZE;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    /**
     * netty编码，发送网络包进行编码
     */
    @Sharable
    private class InternalEncoder extends OneToOneEncoder {

        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel ch, Object msg) throws Exception {
            com.alibaba.dubbo.remoting.buffer.ChannelBuffer buffer =
                com.alibaba.dubbo.remoting.buffer.ChannelBuffers.dynamicBuffer(1024);
            NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
            try {
            	codec.encode(channel, buffer, msg);
            } finally {
                NettyChannel.removeChannelIfDisconnected(ch);
            }
            return ChannelBuffers.wrappedBuffer(buffer.toByteBuffer());
        }
    }

    /**
     * tip:没有@Sharable注解，每当新的channel，都会产生一个实例
     * netty解码，收到网络包进行解码，根据协议解析包,解决粘包拆包现象
     */
    private class InternalDecoder extends SimpleChannelUpstreamHandler {

        private com.alibaba.dubbo.remoting.buffer.ChannelBuffer buffer =
            com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;

        /**
         * 处理消息接收包，主要涉及tcp的粘包和拆包
         * 和协议的解析
         * @param ctx 上下文
         * @param event netty事件
         * @throws Exception
         */
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {

            Object o = event.getMessage();

            //不是ChannelBuffer直接传递给下一个handler处理
            if (! (o instanceof ChannelBuffer)) {
                ctx.sendUpstream(event);
                return;
            }

            //检测通道buffer中是否有可读数据，没有之间返回
            ChannelBuffer input = (ChannelBuffer) o;
            int readable = input.readableBytes();
            if (readable <= 0) {
                return;
            }


            com.alibaba.dubbo.remoting.buffer.ChannelBuffer message;

            //本地buffer中有数据
            if (buffer.readable()) {
                //buffer是DynamicChannelBuffer类型
                //直接写入追加,一个协议包需要至少跨越3三个TCP包承载后发生
                if (buffer instanceof DynamicChannelBuffer) {
                    buffer.writeBytes(input.toByteBuffer());
                    message = buffer;
                } else {
                    //将普通的message实现转化为DynamicChannelBuffer实现，实现处理tcp拆包现象

                    //获得总共大小
                    int size = buffer.readableBytes() + input.readableBytes();
                    //创建DynamicChannelBuffer
                    message = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.dynamicBuffer(
                        size > bufferSize ? size : bufferSize);
                    //将buffer中的东西写入message
                    message.writeBytes(buffer, buffer.readableBytes());
                    //将input中的东西写入message
                    message.writeBytes(input.toByteBuffer());
                }
            } else {
                //是一个数据包开始，因为没有buffer
                //包装netty的buffer为dubbo内部统一的结构处理
                message = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.wrappedBuffer(
                    input.toByteBuffer());
            }

            //获得channel对应netty Channel，也就是每一个netty通道都会拥有一个使用的nettyChanel实例
            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.getChannel(), url, handler);

            Object msg;
            int saveReaderIndex;
            try {
                // decode object.
                do {
                    //buffer有效的可读位置
                    saveReaderIndex = message.readerIndex();
                    try {
                        //通过传递包装的参数（nettyChannel和ChannelBuffers的实现类）
                        // 尝试解码TCP包中的数据，从而获得对象
                        msg = codec.decode(channel, message);
                    } catch (IOException e) {
                        //出错的处理
                        buffer = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                        throw e;
                    }
                    //解码或获得对象是NEED_MORE_INPUT，也就是需要获得更多数据包完成数据的处理
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                        //重置读位置的光标
                        message.readerIndex(saveReaderIndex);
                        break;
                    } else {
                        //校验，先前的读位置的光标和解码之后光标的位置
                        if (saveReaderIndex == message.readerIndex()) {
                            buffer = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                            throw new IOException("Decode without read data.");
                        }
                        if (msg != null) {
                            //传递事件
                            Channels.fireMessageReceived(ctx, msg, event.getRemoteAddress());
                        }
                    }
                } while (message.readable());//下一个可用数据
            } finally {
                if (message.readable()) {
                    message.discardReadBytes();
                    buffer = message;
                } else {
                    buffer = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                }
                NettyChannel.removeChannelIfDisconnected(ctx.getChannel());
            }
        }

        /**
         * 异常处理，
         * 直接传递给下一个handler
         * @param ctx
         * @param e
         * @throws Exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            ctx.sendUpstream(e);
        }
    }
}