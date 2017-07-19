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
 * 作为一个编码编码器，其拥有特定功能编码解码功能这个又Codec2实现，
 * 由于引入dubbo内部对Channel的抽象，因此持有dubbo的handler，用于在编解码前后做出操作
 * URL保存了相关的元信息，
 * buffSize指明了协议包的允许长度，默认是8M，该信息大小也取自url元信息
 *
 * @author qian.lei
 */
final class NettyCodecAdapter {

    private final ChannelHandler encoder = new InternalEncoder();

    private final ChannelHandler decoder = new InternalDecoder();

    private final Codec2 codec;

    private final URL url;

    private final int bufferSize;

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
     * netty编码，对发送网络数据包进行编码
     * 注解@Sharable的存在指明了该通道实例是共享的。
     */
    @Sharable
    private class InternalEncoder extends OneToOneEncoder {

        /**
         * 具体的编码方法。
         * 由于dubbo对网络通讯做了抽象，因此，netty本身基本总是在方法实现内委托
         * dubbo具体实现来操作
         *
         * @param ctx
         * @param ch
         * @param msg
         * @return
         * @throws Exception
         */
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
     * netty解码，netty上行handler，收到网络包进行解码。
     * 根据协议解析包,解决粘包拆包现象。
     * 支持telnet协议。
     */
    private class InternalDecoder extends SimpleChannelUpstreamHandler {

        //用来缓存多于数据，用于粘包拆包现象处理
        private com.alibaba.dubbo.remoting.buffer.ChannelBuffer buffer =
                com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;

        /**
         * 解码上行消息数据包，包括对协议的解析
         * 以及处理粘包拆包现象。
         * 由于dubbo对网络通讯做了抽象，因此，netty本身基本总是在方法实现内委托
         * dubbo具体实现来操作
         *
         * @param ctx   上下文
         * @param event netty事件
         * @throws Exception 异常事件
         */
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {

            Object o = event.getMessage();

            //不是ChannelBuffer直接传递给下一个handler处理
            if (!(o instanceof ChannelBuffer)) {
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

            //本地buffer缓存中有数据，指明发生了粘包拆包现象。需要先处理那部分记录数据
            if (buffer.readable()) {
                //buffer是DynamicChannelBuffer类型
                //直接写入追加,一个协议包需要至少跨越3三个TCP包承载后发生，或者是telnet操作
                if (buffer instanceof DynamicChannelBuffer) {
                    buffer.writeBytes(input.toByteBuffer());
                    message = buffer;
                } else {
                    //将普通的message实现转化为DynamicChannelBuffer实现，实现处理tcp拆包现象

                    //获得缓存数据+buffer中的数据的总大小.
                    int size = buffer.readableBytes() + input.readableBytes();

                    //创建DynamicChannelBuffer,其容量根据总大小和之前的bufferSize大小来确定
                    message = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.dynamicBuffer(
                            size > bufferSize ? size : bufferSize);
                    //1.将缓存数据包中的内容首先中写入message
                    message.writeBytes(buffer, buffer.readableBytes());
                    //2.将网络传递的数据写入message
                    message.writeBytes(input.toByteBuffer());
                }
            }
            //本地buffer中已经没有数据，说明该数据包是一个新的数据包。
            else {
                //尝试将数据包转化为dubbo内部处理的buffer，以便独立与各种网络框架的耦合
                message = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.wrappedBuffer(
                        input.toByteBuffer());
            }

            //获得channel对应netty Channel，也就是每一个netty通道都会拥有一个使用的nettyChanel实例
            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.getChannel(), url, handler);

            Object msg;
            int saveReaderIndex;
            try {
                // decode object.
                //解码对象
                do {
                    //1.message有效的可读的起始位置
                    saveReaderIndex = message.readerIndex();
                    try {
                        //通过传递包装的参数（nettyChannel和ChannelBuffers的实现类）
                        //委托给具体编码解码去处理消息
                        //尝试解码TCP包中的数据，从而获得对象
                        msg = codec.decode(channel, message);
                    } catch (IOException e) {
                        //出错的处理
                        buffer = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                        throw e;
                    }
                    //解码或获得对象是NEED_MORE_INPUT，也就是需要获得更多数据包完成数据的处理
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                        //只要简单重置读的有效起始位置
                        message.readerIndex(saveReaderIndex);
                        break;
                    } else {
                        //校验，先前可读数据的起始位置和解码之后可读数据的位置比较
                        if (saveReaderIndex == message.readerIndex()) {
                            buffer = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                            throw new IOException("Decode without read data.");
                        }
                        //数据包解析过程中有数据对象生成，传递给下一个handler处理
                        if (msg != null) {
                            Channels.fireMessageReceived(ctx, msg, event.getRemoteAddress());
                        }
                    }
                } while (message.readable());//下一个可用数据
            } finally {
                if (message.readable()) {
                    //丢弃可读位置的之前的数据，重置相关偏移量
                    message.discardReadBytes();
                    //赋值给本地缓存，用于后续处理，解决tcp拆包粘包问题。
                    buffer = message;
                } else {
                    //message没有相关数据读取了，之间赋值为空buffer
                    buffer = com.alibaba.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                }
                NettyChannel.removeChannelIfDisconnected(ctx.getChannel());
            }
        }

        /**
         * 异常处理，
         * 直接传递给下一个handler
         *
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