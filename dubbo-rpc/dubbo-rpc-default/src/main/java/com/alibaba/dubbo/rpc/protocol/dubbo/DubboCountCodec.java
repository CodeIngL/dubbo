/*
 * Copyright 1999-2011 Alibaba Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.dubbo.rpc.protocol.dubbo;

import java.io.IOException;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.support.MultiMessage;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;

/**
 * dubbo协议包个数编解码器
 * 尽可能多的尝试编码解码多个dubbo协议包
 * @author <a href="mailto:gang.lvg@alibaba-inc.com">kimi</a>
 */
public final class DubboCountCodec implements Codec2 {

    //dubbo自带编解码器
    private DubboCodec codec = new DubboCodec();

    /**
     * 编码过程委托给codec进行处理
     * @param channel
     * @param buffer
     * @param msg
     * @throws IOException
     */
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        codec.encode(channel, buffer, msg);
    }

    /**
     * 解码
     * 多个协议包 or 单个协议包 or x.x个协议包
     * <ul/>
     * <li>保存buffer可读数据的起始位置</li><br/>
     * <li>构建存储多个协议包的存储对象</li><br/>
     * <li>循环委托给dubboCodec编解码器来进行协议包的解码，每次之多解析出一个协议对象</li><br/>
     * <li>解析协议对象是{@link com.alibaba.dubbo.remoting.Codec2.DecodeResult#NEED_MORE_INPUT},重置buffer在上一个数据包末尾位置</li><br/>
     * <li>解析协议对象是其他独享，尝试加入MultiMessage,并更新可读取的数据位置，尝试读取下一个协议包</li><br/>
     * <li>对于多个消息存储结构处理</li><br/>
     * <li>1.  没有消息，返回{@link com.alibaba.dubbo.remoting.Codec2.DecodeResult#NEED_MORE_INPUT}</li><br/>
     * <li>2.  单个消息，返回单个消息实例</li><br/>
     * <li>3.  多个消息，返回消息存储结构</li><br/>
     * </ul>
     *
     * @param channel 网络抽象channel的包装
     * @param buffer  网络抽象channelBuffer的包装
     * @return
     * @throws IOException
     */
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {

        //buffer中可读数据的起始位置
        int save = buffer.readerIndex();

        //多个数据包存储结构
        MultiMessage result = MultiMessage.create();
        do {
            Object obj = codec.decode(channel, buffer);
            //返回是NEED_MORE_INPUT，说明要读取下个TCP包来获取完整协议
            if (Codec2.DecodeResult.NEED_MORE_INPUT == obj) {
                //重置可读数据起始位置
                buffer.readerIndex(save);
                break;
            } else {
                //添加进result
                result.addMessage(obj);
                logMessageLength(obj, buffer.readerIndex() - save);
                //更新可读数据的位置，一个完整的数据已经被读出，并构建了对象进result中
                save = buffer.readerIndex();
            }
        } while (true);
        //对于消息对象为0的情况处理
        if (result.isEmpty()) {
            return Codec2.DecodeResult.NEED_MORE_INPUT;
        }
        //对于消息对象为1的情况处理
        if (result.size() == 1) {
            return result.get(0);
        }
        //对于消息对象为多个的情况处理
        return result;
    }

    /**
     * 设置信息，为消息信息添加额外的消息长度信息。
     * <ul>
     * <li>对于是请求结构，为请求的内部数据{@link RpcInvocation}增加键值对{@link Constants#INPUT_KEY},bytes</li><br/>
     * <li>对于是响应结构，为响应的内部数据{@link RpcResult}增加键值对{@link Constants#OUTPUT_KEY},bytes</li><br/>
     * </ul>
     *
     * @param result
     * @param bytes
     */
    private void logMessageLength(Object result, int bytes) {
        if (bytes <= 0) {
            return;
        }
        if (result instanceof Request) {
            try {
                ((RpcInvocation) ((Request) result).getData()).setAttachment(
                        Constants.INPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        } else if (result instanceof Response) {
            try {
                ((RpcResult) ((Response) result).getResult()).setAttachment(
                        Constants.OUTPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        }
    }

}
