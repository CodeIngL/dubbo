package com.alibaba.dubbo.remoting.zookeeper;

import java.util.List;

/**
 * zookeeper的listener的抽象，实际操作由特定的zookeeper客户端决定
 */
public interface ChildListener {

    /**
     * 子节点发生变化时回调
     * @param path 父路径
     * @param children 父路径对应的所有子节点
     */
    void childChanged(String path, List<String> children);

}
