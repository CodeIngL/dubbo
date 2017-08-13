package com.alibaba.dubbo.remoting.zookeeper;

import java.util.List;

/**
 * zookeeper的listener的抽象，实际操作由特定的zookeeper客户端决定
 */
public interface ChildListener {

    void childChanged(String path, List<String> children);

}
