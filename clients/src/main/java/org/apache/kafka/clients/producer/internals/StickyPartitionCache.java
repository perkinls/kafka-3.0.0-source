/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Utils;

/**
 * 粘性分区缓存内部类，用于记录给定topic的粘性分区
 * An internal class that implements a cache used for sticky partitioning behavior. The cache tracks the current sticky
 * partition for any given topic. This class should not be used externally. 
 */
public class StickyPartitionCache {
    private final ConcurrentMap<String, Integer> indexCache;
    public StickyPartitionCache() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    public int partition(String topic, Cluster cluster) {
        Integer part = indexCache.get(topic);
        if (part == null) {
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    /**
     *
     * 指定partition直接发送到该partition中；
     * 未指定partition，指定了key，则按key哈希后与元数据中所有partition数量取模；
     * 未指定partition，未指定key，则随机发送。
     * @param topic 主题名称
     * @param cluster 当前集群元数据
     * @param prevPartition 先前为触发新批次的记录选择的分区
     * @return
     */
    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic); // 根据元数据获取分区信息
        Integer oldPart = indexCache.get(topic); // 粘性分区上一次的分区记录
        Integer newPart = oldPart;
        // 检查topic的当前粘性分区是否未设置，或者触发新批处理的分区与需要更改的粘性分区匹配。
        if (oldPart == null || oldPart == prevPartition) {
            // 从元数据中获取该topic中可用的partition
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() < 1) { //  没有可用的partition，从所有partition中随机选择一个（与所有partition取模）
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();
            } else if (availablePartitions.size() == 1) { // 只有一个可用的partition
                newPart = availablePartitions.get(0).partition();
            } else { // 有多个可用的partition，随机获取一个与缓存中不相同的partition
                while (newPart == null || newPart.equals(oldPart)) {
                    int random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }
            // Only change the sticky partition if it is null or prevPartition matches the current sticky partition.
            if (oldPart == null) {
                indexCache.putIfAbsent(topic, newPart);
            } else {
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }
        return indexCache.get(topic);
    }

}