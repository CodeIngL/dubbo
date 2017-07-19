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
package com.alibaba.dubbo.remoting.telnet.codec;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.transport.codec.TransportCodec;

/**
 * TelnetCodec
 * 
 * @author heyman
 * @author william.liangf
 * @author chao.liuc
 */
public class TelnetCodec extends TransportCodec {

    private static final Logger  logger = LoggerFactory.getLogger(TelnetCodec.class);
    
    private static final String HISTORY_LIST_KEY = "telnet.history.list";

    private static final String HISTORY_INDEX_KEY = "telnet.history.index";
    
    private static final byte[] UP = new byte[] {27, 91, 65};
    
    private static final byte[] DOWN = new byte[] {27, 91, 66};

    private static final List<?> ENTER  = Arrays.asList(new Object[] { new byte[] { '\r', '\n' } /* Windows Enter */, new byte[] { '\n' } /* Linux Enter */ });

    private static final List<?> EXIT   = Arrays.asList(new Object[] { new byte[] { 3 } /* Windows Ctrl+C */, new byte[] { -1, -12, -1, -3, 6 } /* Linux Ctrl+C */, new byte[] { -1, -19, -1, -3, 6 } /* Linux Pause */ });

    public void encode(Channel channel, ChannelBuffer buffer, Object message) throws IOException {
        if (message instanceof String) {
            if (isClientSide(channel)) {
                message = message + "\r\n";
            }
            byte[] msgData = ((String) message).getBytes(getCharset(channel).name());
            buffer.writeBytes(msgData);
        } else {
            super.encode(channel, buffer, message);
        }
    }
    
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        int readable = buffer.readableBytes();
        byte[] message = new byte[readable];
        buffer.readBytes(message);
        return decode(channel, buffer, readable, message);
    }

    /**
     * message代表自己数组成功的包含从当前位置（非协议头）开始到一个dubbo协议包末尾，或者dubbo协议包过长，还未包含到末尾，或者是telenet命令
     * 对于前两者，最终会返回{@link com.alibaba.dubbo.remoting.Codec2.DecodeResult#NEED_MORE_INPUT}
     * telnet的处理
     * @param channel 网络抽象channel的包装
     * @param buffer 网络抽象channelBuffer的包装
     * @param readable buffer中可读的数据长度
     * @param message  包含从当前位置（非协议头）开始到一个dubbo协议包末尾，或者dubbo协议包过长，还未包含到末尾的字节数组
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] message) throws IOException {
        //判断是服务提供者，还是消费者，通过side判断
        if (isClientSide(channel)) {
            return toString(message, getCharset(channel));
        }
        //检查payload部分
        checkPayload(channel, readable);

        //检查message
        if (message == null || message.length == 0) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        //含有回显符号，直接发回去，放回NEED_MORE_INPUT
        if (message[message.length - 1] == '\b') { // Windows backspace echo
            try {
                boolean doublechar = message.length >= 3 && message[message.length - 3] < 0; // double byte char
                channel.send(new String(doublechar ? new byte[] {32, 32, 8, 8} : new byte[] {32, 8}, getCharset(channel).name()));
            } catch (RemotingException e) {
                throw new IOException(StringUtils.toString(e));
            }
            //还需要更多数据
            return DecodeResult.NEED_MORE_INPUT;
        }

        //含有结束命令，关闭通道，返回null
        for (Object command : EXIT) {
            if (isEquals(message, (byte[]) command)) {
                if (logger.isInfoEnabled()) {
                    logger.info(new Exception("Close channel " + channel + " on exit command: " + Arrays.toString((byte[])command)));
                }
                channel.close();
                return null;
            }
        }

        //messgae带有用户按动向上键或者向下键的处理
        boolean up = endsWith(message, UP);
        boolean down = endsWith(message, DOWN);
        if (up || down) {
            //历史命令，从缓存中获得用户历史命令
            LinkedList<String> history = (LinkedList<String>) channel.getAttribute(HISTORY_LIST_KEY);
            //没有历史命令，直接返回NEED_MORE_INPUT
            if (history == null || history.size() == 0) {
                return DecodeResult.NEED_MORE_INPUT;
            }
            //获得当前的命令位置
            Integer index = (Integer) channel.getAttribute(HISTORY_INDEX_KEY);
            Integer old = index;
            if (index == null) {
                //如果当前命令位置没有，直接使用最后一个命令
                index = history.size() - 1;
            } else {
                if (up) {
                    //向上键的处理
                    index = index - 1;
                    if (index < 0) {
                        index = history.size() - 1;
                    }
                } else {
                    //向下键的处理
                    index = index + 1;
                    if (index > history.size() - 1) {
                        index = 0;
                    }
                }
            }
            //命令发生改变，重写缓存隶属
            if (old == null || ! old.equals(index)) {
                channel.setAttribute(HISTORY_INDEX_KEY, index);
                String value = history.get(index);
                if (old != null && old >= 0 && old < history.size()) {
                    String ov = history.get(old);
                    StringBuilder buf = new StringBuilder();
                    for (int i = 0; i < ov.length(); i ++) {
                        buf.append("\b");
                    }
                    for (int i = 0; i < ov.length(); i ++) {
                        buf.append(" ");
                    }
                    for (int i = 0; i < ov.length(); i ++) {
                        buf.append("\b");
                    }
                    value = buf.toString() + value;
                }
                try {
                    channel.send(value);
                } catch (RemotingException e) {
                    throw new IOException(StringUtils.toString(e));
                }
            }
            return DecodeResult.NEED_MORE_INPUT;
        }
        //含有结束命令，关闭通道，返回null
        for (Object command : EXIT) {
            if (isEquals(message, (byte[]) command)) {
                if (logger.isInfoEnabled()) {
                    logger.info(new Exception("Close channel " + channel + " on exit command " + command));
                }
                channel.close();
                return null;
            }
        }
        byte[] enter = null;
        for (Object command : ENTER) {
            if (endsWith(message, (byte[]) command)) {
                enter = (byte[]) command;
                break;
            }
        }
        if (enter == null) {
            return DecodeResult.NEED_MORE_INPUT;
        }
        LinkedList<String> history = (LinkedList<String>) channel.getAttribute(HISTORY_LIST_KEY);
        Integer index = (Integer) channel.getAttribute(HISTORY_INDEX_KEY);
        channel.removeAttribute(HISTORY_INDEX_KEY);
        if (history != null && history.size() > 0 && index != null && index >= 0 && index < history.size()) {
            String value = history.get(index);
            if (value != null) {
                byte[] b1 = value.getBytes();
                if (message != null && message.length > 0) {
                    byte[] b2 = new byte[b1.length + message.length];
                    System.arraycopy(b1, 0, b2, 0, b1.length);
                    System.arraycopy(message, 0, b2, b1.length, message.length);
                    message = b2;
                } else {
                    message = b1;
                }
            }
        }
        String result = toString(message, getCharset(channel));
        if (result != null && result.trim().length() > 0) {
            if (history == null) {
                history = new LinkedList<String>();
                channel.setAttribute(HISTORY_LIST_KEY, history);
            }
            if (history.size() == 0) {
                history.addLast(result);
            } else if (! result.equals(history.getLast())) {
                history.remove(result);
                history.addLast(result);
                if (history.size() > 10) {
                    history.removeFirst();
                }
            }
        }
        //成功最后返回的String字符串，这和其他是不同的。
        return result;
    }
    
    private static Charset getCharset(Channel channel) {
        if (channel != null) {
            Object attribute = channel.getAttribute(Constants.CHARSET_KEY);
            if (attribute instanceof String) {
                try {
                    return Charset.forName((String) attribute);
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            } else if (attribute instanceof Charset) {
                return (Charset) attribute;
            }
            URL url = channel.getUrl();
            if (url != null) {
                String parameter = url.getParameter(Constants.CHARSET_KEY);
                if (parameter != null && parameter.length() > 0) {
                    try {
                        return Charset.forName(parameter);
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            }
        }
        try {
            return Charset.forName("GBK");
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        return Charset.defaultCharset();
    }

    private static String toString(byte[] message, Charset charset) throws UnsupportedEncodingException {
        byte[] copy = new byte[message.length];
        int index = 0;
        for (int i = 0; i < message.length; i ++) {
            byte b = message[i] ;
            if (b == '\b') { // backspace
                if (index > 0) {
                    index --;
                }
                if (i > 2 && message[i - 2] < 0) { // double byte char
                    if (index > 0) {
                        index --;
                    }
                }
            } else if (b == 27) { // escape
                if (i < message.length - 4 && message[i + 4] == 126) {
                    i = i + 4;
                } else if (i < message.length - 3 && message[i + 3] == 126) {
                    i = i + 3;
                } else if (i < message.length - 2) {
                    i = i + 2;
                }
            } else if (b == -1 && i < message.length - 2 
                    && (message[i + 1] == -3 || message[i + 1] == -5)) { // handshake
                i = i + 2;
            } else {
                copy[index ++] = message[i];
            }
        }
        if (index == 0) {
            return "";
        }
        return new String(copy, 0, index, charset.name()).trim();
    }

    private static boolean isEquals(byte[] message, byte[] command) throws IOException {
        return message.length == command.length && endsWith(message, command);
    }

    private static boolean endsWith(byte[] message, byte[] command) throws IOException {
        if (message.length < command.length) {
            return false;
        }
        int offset = message.length - command.length;
        for (int i = command.length - 1; i >= 0 ; i --) {
            if (message[offset + i] != command[i]) {
                return false;
            }
        }
        return true;
    }

}