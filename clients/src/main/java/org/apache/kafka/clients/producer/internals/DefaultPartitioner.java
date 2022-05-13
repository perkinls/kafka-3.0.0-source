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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * 默认分区策略：
 * 如果记录中指定了分区，使用指定分区
 * 如果未指定分区但存在键，则根据键的散列选择分区
 * 如果不存在分区或键，则选择在批处理已满时更改的粘性分区。有关粘性分区的详细信息，请参阅 KIP-480。
 */
public class DefaultPartitioner implements Partitioner {

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    public void configure(Map<String, ?> configs) {
    }

    /**
     * 计算给定记录的分区。
     *
     * @param topic      – 主题名称
     * @param key        – 要分区的键（如果没有键，则为 null）
     * @param keyBytes   – 分区的序列化键（如果没有键，则为 null）
     * @param value      - 要分区的值或 null
     * @param valueBytes – 要分区的序列化值或 null
     * @param cluster    – 当前集群元数据
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return partition(topic, key, keyBytes, value, valueBytes, cluster, cluster.partitionsForTopic(topic).size());
    }

    /**
     * Compute the partition for the given record.
     *
     * @param topic         The topic name
     * @param numPartitions The number of partitions of the given {@code topic}
     * @param key           The key to partition on (or null if no key)
     * @param keyBytes      serialized key to partition on (or null if no key)
     * @param value         The value to partition on or null
     * @param valueBytes    serialized value to partition on or null
     * @param cluster       The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster,
                         int numPartitions) {
        // 没有指定key
        if (keyBytes == null) {
            // 按照粘性分区处理
            return stickyPartitionCache.partition(topic, cluster);
        }
        // 如果指定key,按照key的hashcode值 对分区数求模
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    public void close() {
    }

    /**
     * 如果当前粘性分区的批处理已完成，请更改粘性分区。或者，如果没有确定粘性分区，则设置一个。
     * If a batch completed for the current sticky partition, change the sticky partition.
     * Alternately, if no sticky partition has been determined, set one.
     */
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        stickyPartitionCache.nextPartition(topic, cluster, prevPartition);
    }
}
