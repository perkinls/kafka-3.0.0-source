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
package org.apache.kafka.clients;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 * Kafka 集群中节点、主题和分区的内部可变缓存。这会保持一个最新的集群实例，该实例针对读取访问进行了优化。
 * Kafka 集群中关于 node、topic 和 partition 的信息。（是只读的）
 */
public class MetadataCache {
    private final String clusterId;
    private final Map<Integer, Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, PartitionMetadata> metadataByPartition;
    private final Map<String, Uuid> topicIds;
    private Cluster clusterInstance;


    /**
     * 在KafkaProduce初始化过程中 {@link org.apache.kafka.clients.producer.KafkaProducer#KafkaProducer(Map)}
     * 会调用metadata.bootstrap(addresses) 一步一步进入到该构造函数
     */
    MetadataCache(String clusterId,
                  Map<Integer, Node> nodes,
                  Collection<PartitionMetadata> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller,
                  Map<String, Uuid> topicIds) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, topicIds, null);
    }

    private MetadataCache(String clusterId,
                          Map<Integer, Node> nodes,
                          Collection<PartitionMetadata> partitions,
                          Set<String> unauthorizedTopics,
                          Set<String> invalidTopics,
                          Set<String> internalTopics,
                          Node controller,
                          Map<String, Uuid> topicIds,
                          Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = nodes;
        this.unauthorizedTopics = unauthorizedTopics;
        this.invalidTopics = invalidTopics;
        this.internalTopics = internalTopics;
        this.controller = controller;
        this.topicIds = topicIds;

        this.metadataByPartition = new HashMap<>(partitions.size());
        for (PartitionMetadata p : partitions) {
            this.metadataByPartition.put(p.topicPartition, p);
        }

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    Optional<PartitionMetadata> partitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition));
    }

    Uuid topicId(String topicName) {
        return topicIds.get(topicName);
    }

    Optional<Node> nodeById(int id) {
        return Optional.ofNullable(nodes.get(id));
    }

    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    ClusterResource clusterResource() {
        return new ClusterResource(clusterId);
    }

    /**
     * Merges the metadata cache's contents with the provided metadata, returning a new metadata cache. The provided
     * metadata is presumed to be more recent than the cache's metadata, and therefore all overlapping metadata will
     * be overridden.
     *
     * @param newClusterId          the new cluster Id
     * @param newNodes              the new set of nodes
     * @param addPartitions         partitions to add
     * @param addUnauthorizedTopics unauthorized topics to add
     * @param addInternalTopics     internal topics to add
     * @param newController         the new controller node
     * @param topicIds              the mapping from topic name to topic ID from the MetadataResponse
     * @param retainTopic           returns whether a topic's metadata should be retained
     * @return the merged metadata cache
     */
    MetadataCache mergeWith(String newClusterId,
                            Map<Integer, Node> newNodes,
                            Collection<PartitionMetadata> addPartitions,
                            Set<String> addUnauthorizedTopics,
                            Set<String> addInvalidTopics,
                            Set<String> addInternalTopics,
                            Node newController,
                            Map<String, Uuid> topicIds,
                            BiPredicate<String, Boolean> retainTopic) {

        Predicate<String> shouldRetainTopic = topic -> retainTopic.test(topic, internalTopics.contains(topic));

        Map<TopicPartition, PartitionMetadata> newMetadataByPartition = new HashMap<>(addPartitions.size());
        Map<String, Uuid> newTopicIds = new HashMap<>(topicIds.size());

        // We want the most recent topic ID. We start with the previous ID stored for retained topics and then
        // update with newest information from the MetadataResponse. We always take the latest state, removing existing
        // topic IDs if the latest state contains the topic name but not a topic ID.
        this.topicIds.forEach((topicName, topicId) -> {
            if (shouldRetainTopic.test(topicName)) {
                newTopicIds.put(topicName, topicId);
            }
        });

        for (PartitionMetadata partition : addPartitions) {
            newMetadataByPartition.put(partition.topicPartition, partition);
            Uuid id = topicIds.get(partition.topic());
            if (id != null)
                newTopicIds.put(partition.topic(), id);
            else
                // Remove if the latest metadata does not have a topic ID
                newTopicIds.remove(partition.topic());
        }
        for (Map.Entry<TopicPartition, PartitionMetadata> entry : metadataByPartition.entrySet()) {
            if (shouldRetainTopic.test(entry.getKey().topic())) {
                newMetadataByPartition.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        Set<String> newUnauthorizedTopics = fillSet(addUnauthorizedTopics, unauthorizedTopics, shouldRetainTopic);
        Set<String> newInvalidTopics = fillSet(addInvalidTopics, invalidTopics, shouldRetainTopic);
        Set<String> newInternalTopics = fillSet(addInternalTopics, internalTopics, shouldRetainTopic);

        return new MetadataCache(newClusterId, newNodes, newMetadataByPartition.values(), newUnauthorizedTopics,
                newInvalidTopics, newInternalTopics, newController, newTopicIds);
    }

    /**
     * Copies {@code baseSet} and adds all non-existent elements in {@code fillSet} such that {@code predicate} is true.
     * In other words, all elements of {@code baseSet} will be contained in the result, with additional non-overlapping
     * elements in {@code fillSet} where the predicate is true.
     *
     * @param baseSet   the base elements for the resulting set
     * @param fillSet   elements to be filled into the resulting set
     * @param predicate tested against the fill set to determine whether elements should be added to the base set
     */
    private static <T> Set<T> fillSet(Set<T> baseSet, Set<T> fillSet, Predicate<T> predicate) {
        Set<T> result = new HashSet<>(baseSet);
        for (T element : fillSet) {
            if (predicate.test(element)) {
                result.add(element);
            }
        }
        return result;
    }

    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(metadata -> MetadataResponse.toPartitionInfo(metadata, nodes))
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes.values(), partitionInfos, unauthorizedTopics,
                invalidTopics, internalTopics, controller, topicIds);
    }

    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        Map<Integer, Node> nodes = new HashMap<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses) {
            nodes.put(nodeId, new Node(nodeId, address.getHostString(), address.getPort()));
            nodeId--;
        }

        return new MetadataCache(null,
                nodes,
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null,
                Collections.emptyMap(),
                Cluster.bootstrap(addresses));
    }

    static MetadataCache empty() {
        return new MetadataCache(null,
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null,
                Collections.emptyMap(),
                Cluster.empty()
        );
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "clusterId='" + clusterId + '\'' +
                ", nodes=" + nodes +
                ", partitions=" + metadataByPartition.values() +
                ", controller=" + controller +
                '}';
    }

}