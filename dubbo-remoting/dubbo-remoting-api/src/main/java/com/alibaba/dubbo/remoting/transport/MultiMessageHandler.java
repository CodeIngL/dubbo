package com.alibaba.dubbo.remoting.transport;

import com.alibaba.dubbo.remoting.exchange.support.MultiMessage;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.support.MultiMessage;

/**
 * @author <a href="mailto:gang.lvg@alibaba-inc.com">kimi</a>
 * @see MultiMessage
 */
public class MultiMessageHandler extends AbstractChannelHandlerDelegate {

    public MultiMessageHandler(ChannelHandler handler) {
        super(handler);
    }

    /**
     * 除去特定网络抽象handler，后第一个入口处理,默认情况下
     * ex 针对netty来说，NettyCodeCAdapter-->NettyHandler-->MultiMessageHandler
     * @param channel
     * @param message
     * @throws RemotingException
     */
    @SuppressWarnings("unchecked")
	@Override
    public void received(Channel channel, Object message) throws RemotingException {
        //处理多个消息
        if (message instanceof MultiMessage) {
            MultiMessage list = (MultiMessage)message;
            for(Object obj : list) {
                handler.received(channel, obj);
            }
        } else {
            //处理单个消息
            handler.received(channel, message);
        }
    }
}
