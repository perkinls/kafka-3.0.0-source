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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private final Logger log;

    /* 网络通信客户端，主要封装与 broker 的网络通信 */
    private final KafkaClient client;

    /* 消息记录累积器，消息追加的入口(RecordAccumulator 的 append 方法)。 */
    private final RecordAccumulator accumulator;

    /* 元数据管理器，即 topic 的路由分区信息 */
    private final ProducerMetadata metadata;

    /* 是否需要保证消息的顺序性 */
    private final boolean guaranteeMessageOrder;

    /* 调用 send 方法发送的最大请求大小，包括 key、消息体序列化后的消息总大小不能超过该值。通过参数 max.request.size 来设置 */
    private final int maxRequestSize;

    /* 用来定义消息“已提交”的条件(标准)，就是 Broker 端向客户端承偌已提交的条件，可选值如下0、-1、1. */
    private final short acks;

    /* 重试次数 */
    private final int retries;

    /* 时间工具类 */
    private final Time time;

    /* 该线程状态，为 true 表示运行中 */
    private volatile boolean running;

    /* 是否强制关闭，此时会忽略正在发送中的消息.  */
    private volatile boolean forceClose;

    /* 消息发送相关的统计指标收集器 */
    private final SenderMetrics sensors;

    /* 请求的超时时间 */
    private final int requestTimeoutMs;

    /* 请求失败之在重试之前等待的时间 */
    private final long retryBackoffMs;

    /* API版本信息 */
    private final ApiVersions apiVersions;

    /* 事务处理器 */
    private final TransactionManager transactionManager;

    /* 正在执行发送相关的消息批次 */
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    public Sender(LogContext logContext,
                  KafkaClient client,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager,
                  ApiVersions apiVersions) {
        this.log = logContext.logger(Sender.class);
        this.client = client; // NetworkClient
        this.accumulator = accumulator; // 消息缓存器
        this.metadata = metadata; // 元数据
        this.guaranteeMessageOrder = guaranteeMessageOrder; // 是否保证消息舒徐
        this.maxRequestSize = maxRequestSize; // 单消息最大请求大小
        this.running = true; // 标志位
        this.acks = acks; // ack确认机制
        this.retries = retries; // 重试次数
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
    }

    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    private void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            batches.remove(batch);
            if (batches.isEmpty()) {
                inFlightBatches.remove(batch.topicPartition);
            }
        }
    }

    private void maybeRemoveAndDeallocateBatch(ProducerBatch batch) {
        maybeRemoveFromInflightBatches(batch);
        this.accumulator.deallocate(batch);
    }

    /**
     * Get the in-flight batches that has reached delivery timeout.
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();

        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext(); ) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();

            if (partitionInFlightBatches != null) {
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
                    // 判断正在执行批次的记录是否过期 delivery.timeout.ms
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        iter.remove();
                        // expireBatches is called in Sender.sendProducerData, before client.poll.
                        // The !batch.isDone() invariant should always hold. An IllegalStateException
                        // exception will be thrown if the invariant is violated.
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                    batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
                if (partitionInFlightBatches.isEmpty()) {
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            List<ProducerBatch> inflightBatchList = inFlightBatches.get(batch.topicPartition);
            if (inflightBatchList == null) {
                inflightBatchList = new ArrayList<>();
                inFlightBatches.put(batch.topicPartition, inflightBatchList);
            }
            inflightBatchList.add(batch);
        }
    }

    public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
        for (List<ProducerBatch> batchList : batches.values()) {
            addToInflightBatches(batchList);
        }
    }

    private boolean hasPendingTransactionalRequests() {
        return transactionManager != null && transactionManager.hasPendingRequests() && transactionManager.hasOngoingTransaction();
    }

    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // 主循环，运行知道 close 被调用
        while (running) { // running 标志位,初始化默认true
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        //停止接受请求，但事务管理器，累加器中可能还有请求，等待确认，直到这些完成。
        while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // 如果任何提交或中止未通过事务管理器的队列,则中止事务
        while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
            if (!transactionManager.isCompleting()) {
                log.info("Aborting incomplete transaction due to shutdown");
                transactionManager.beginAbort();
            }
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        if (forceClose) {
            // 如果强制关闭,需要使所有未完成的事务关闭以及中止不完整的批次,并唤醒等待线程
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown");
                transactionManager.close();
            }
            log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     */
    void runOnce() {

        /*
         *  开启了事务,进行事务处理
         */
        if (transactionManager != null) {
            try {
                transactionManager.maybeResolveSequences();

                // 如果事务管理器处于失败状态,则不要继续发送
                if (transactionManager.hasFatalError()) {
                    RuntimeException lastError = transactionManager.lastError();
                    if (lastError != null)
                        maybeAbortBatches(lastError);
                    client.poll(retryBackoffMs, time.milliseconds());
                    return;
                }

                // 检查我们是否需要一个新的producerId. 如果是这样,我们将排队一个InitProducerId请求,该请求将在下面发送.
                transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();

                if (maybeSendAndPollTransactionalRequest()) {
                    return;
                }
            } catch (AuthenticationException e) {
                // This is already logged as error, but propagated here to perform any clean ups.
                log.trace("Authentication exception while processing transactional request", e);
                transactionManager.authenticationFailed(e);
            }
        }

        long currentTimeMs = time.milliseconds();

        /* 核心逻辑: 发送 Producer 请求，这个方法会调用 NetworkClient.send() 来发送 clientRequest*/
        long pollTimeout = sendProducerData(currentTimeMs);

        /* 关于 socket 的一些实际的读写操作，这个方法会继续调用 Kafka 封装的 Selector.poll()，跟进去底层是调用的 Java NIO 的 Selector.poll() */
        client.poll(pollTimeout, currentTimeMs);
    }


    /**
     * 消息发送逻辑方法
     *
     * @param now 房前时间戳
     * @return
     */
    private long sendProducerData(long now) {

        // 获取集群元数据
        Cluster cluster = metadata.fetch();

        // 1. 根据可以发送条件, 获取 accumulator 准备好可以发送消息的分区列表信息
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        // 2. 如果有topic没有leader强制更新元数据
        if (!result.unknownLeaderTopics.isEmpty()) {
            // 没有leader的场景例如leader选举，或者topic已失效，这些都需要将topic重新加入，
            // 发送到服务端请求更新，因为现在还需要往这些topic发送消息
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic, now);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}", result.unknownLeaderTopics);
            this.metadata.requestUpdate();
        }

        // 3. 如果与Node没有连接(如果可以连接,同时初始化该连接),不能连接就证明该节点暂时不能发送数据,暂时移除该节点
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE; // 预估分区在接下来多久的时间间隔内都将处于未转变好状态
        while (iter.hasNext()) {
            Node node = iter.next();
            // 启动给定节点的连接,如果给定的节点没连接到,则会初始化启动连接
            if (!this.client.ready(node, now)) {
                iter.remove(); // 没建立好的连接移除集合

                notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now)); //pollDelayMs
            }
        }

        // 4. 返回每个node节点对应的ProducerBatch数据列表(key 是node.id,这些 batches 将会在一个request中发送)
        Map<Integer, List<ProducerBatch>> batches =
                this.accumulator.drain(cluster, result.readyNodes, this.maxRequestSize, now);

        // 数据按照 TopicPartition 写入 inFlightBatches (正在执行的批次数据集合)
        addToInflightBatches(batches);

        // 保证有序性, 保证一个 TopicPartition 只有一个 RecordBatch 在发送. (max.in.flights.request.per.connection) 设置为1时会保证
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }


        accumulator.resetNextBatchExpiryTime();


        // 筛选已过期的批次
        List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
        expiredBatches.addAll(expiredInflightBatches);

        /*
         * 如果之前已将过期批次发送到broker，请重置生产者 ID。同时更新过期批次的指标。
         * 请参阅 @TransactionState.resetIdempotentProducerId 的文档以了解为什么我们需要在此处重置生产者 id。
         */
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        for (ProducerBatch expiredBatch : expiredBatches) {
            String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                    + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
            failBatch(expiredBatch, new TimeoutException(errorMessage), false);
            if (transactionManager != null && expiredBatch.inRetry()) {
                // 确保在当前运行中的批次完全解决之前不会耗尽新批次
                transactionManager.markSequenceUnresolved(expiredBatch);
            }
        }
        sensors.updateProduceRequestMetrics(batches);


        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
        pollTimeout = Math.max(pollTimeout, 0);

        /*
         * 如果我们有任何节点准备发送 + 有可发送的数据，以0超时轮询，这样可以立即循环并尝试发送更多数据。
         * 否则，超时时间为下一次批处理过期时间与数据可用性检查延迟时间之间的较小值。请注意，节点可能有由于延迟、退出等原因而无法发送的数据。
         * 这特别不包括那些没有准备好发送的可发送数据的节点，因为它们会导致繁忙的循环。
         */
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            /*
             * 如果某些分区已经准备好要发送，则选择时间为0;否则，如果某个分区已经积累了一些数据，但还没有准备好，
             * 则选择的时间将是现在与其逗留过期时间之间的时间差;否则，选择的时间将是现在与元数据过期时间之间的时间差;
             */
            pollTimeout = 0;
        }


        /*
         * 这里按照节点ID对应批次消息构建发送请求，每一个节点ID将会对应多个 ProducerBatch 一起封装成一个请求进行发送，
         * 同一时间，对应服务器的节点连接只会只能发送一个请求，需要注意这里只是用于构建请求，会将数据排队待发送中，并不是真正的数据发送
         */
        sendProduceRequests(batches, now);
        return pollTimeout;
    }

    /**
     * Returns true if a transactional request is sent or polled, or if a FindCoordinator request is enqueued
     */
    private boolean maybeSendAndPollTransactionalRequest() {
        if (transactionManager.hasInFlightRequest()) {
            // as long as there are outstanding transactional requests, we simply wait for them to return
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        }

        if (transactionManager.hasAbortableError() || transactionManager.isAborting()) {
            if (accumulator.hasIncomplete()) {
                // Attempt to get the last error that caused this abort.
                RuntimeException exception = transactionManager.lastError();
                // If there was no error, but we are still aborting,
                // then this is most likely a case where there was no fatal error.
                if (exception == null) {
                    exception = new TransactionAbortedException();
                }
                accumulator.abortUndrainedBatches(exception);
            }
        }

        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        Node targetNode = null;
        try {
            FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
            targetNode = coordinatorType != null ?
                    transactionManager.coordinator(coordinatorType) :
                    client.leastLoadedNode(time.milliseconds());
            if (targetNode != null) {
                if (!awaitNodeReady(targetNode, coordinatorType)) {
                    log.trace("Target node {} not ready within request timeout, will retry when node is ready.", targetNode);
                    maybeFindCoordinatorAndRetry(nextRequestHandler);
                    return true;
                }
            } else if (coordinatorType != null) {
                log.trace("Coordinator not known for {}, will retry {} after finding coordinator.", coordinatorType, requestBuilder.apiKey());
                maybeFindCoordinatorAndRetry(nextRequestHandler);
                return true;
            } else {
                log.trace("No nodes available to send requests, will poll and retry when until a node is ready.");
                transactionManager.retry(nextRequestHandler);
                client.poll(retryBackoffMs, time.milliseconds());
                return true;
            }

            if (nextRequestHandler.isRetry())
                time.sleep(nextRequestHandler.retryBackoffMs());

            long currentTimeMs = time.milliseconds();
            ClientRequest clientRequest = client.newClientRequest(targetNode.idString(), requestBuilder, currentTimeMs,
                    true, requestTimeoutMs, nextRequestHandler);
            log.debug("Sending transactional request {} to node {} with correlation ID {}", requestBuilder, targetNode, clientRequest.correlationId());
            client.send(clientRequest, currentTimeMs);
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        } catch (IOException e) {
            log.debug("Disconnect from {} while trying to send request {}. Going " +
                    "to back off and retry.", targetNode, requestBuilder, e);
            // We break here so that we pick up the FindCoordinator request immediately.
            maybeFindCoordinatorAndRetry(nextRequestHandler);
            return true;
        }
    }

    private void maybeFindCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
        if (nextRequestHandler.needsCoordinator()) {
            transactionManager.lookupCoordinator(nextRequestHandler);
        } else {
            // For non-coordinator requests, sleep here to prevent a tight loop when no node is available
            time.sleep(retryBackoffMs);
            metadata.requestUpdate();
        }

        transactionManager.retry(nextRequestHandler);
    }

    private void maybeAbortBatches(RuntimeException exception) {
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception);
            accumulator.abortBatches(exception);
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    public boolean isRunning() {
        return running;
    }

    private boolean awaitNodeReady(Node node, FindCoordinatorRequest.CoordinatorType coordinatorType) throws IOException {
        if (NetworkClientUtils.awaitReady(client, node, time, requestTimeoutMs)) {
            if (coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
                // Indicate to the transaction manager that the coordinator is ready, allowing it to check ApiVersions
                // This allows us to bump transactional epochs even if the coordinator is temporarily unavailable at
                // the time when the abortable error is handled
                transactionManager.handleCoordinatorReady();
            }
            return true;
        }
        return false;
    }


    /**
     * 处理回调响应
     * @param response ClientResponse
     * @param batches 分区对应记录
     * @param now 当前时间戳
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
        RequestHeader requestHeader = response.requestHeader();
        int correlationId = requestHeader.correlationId();
        if (response.wasDisconnected()) {
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                    requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION, String.format("Disconnected from node %s", response.destination())),
                        correlationId, now);
        } else if (response.versionMismatch() != null) {
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now);
        } else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            if (response.hasResponse()) {
                // Sender should exercise PartitionProduceResponse rather than ProduceResponse.PartitionResponse
                // https://issues.apache.org/jira/browse/KAFKA-10696
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                produceResponse.data().responses().forEach(r -> r.partitionResponses().forEach(p -> {
                    TopicPartition tp = new TopicPartition(r.name(), p.index());
                    ProduceResponse.PartitionResponse partResp = new ProduceResponse.PartitionResponse(
                            Errors.forCode(p.errorCode()),
                            p.baseOffset(),
                            p.logAppendTimeMs(),
                            p.logStartOffset(),
                            p.recordErrors()
                                    .stream()
                                    .map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage()))
                                    .collect(Collectors.toList()),
                            p.errorMessage());
                    ProducerBatch batch = batches.get(tp);
                    completeBatch(batch, partResp, correlationId, now);
                }));
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // this is the acks = 0 case, just complete all requests
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch         The record batch
     * @param response      The produce response
     * @param correlationId The correlation id for the request
     * @param now           The current POSIX timestamp in milliseconds
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now) {
        Errors error = response.error;

        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // If the batch is too large, we split the batch and send the split batches again. We do not decrement
            // the retry attempts in this case.
            log.warn(
                    "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts(),
                    formatErrMsg(response));
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            this.accumulator.splitAndReenqueue(batch);
            maybeRemoveAndDeallocateBatch(batch);
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) {
            if (canRetry(batch, response, now)) {
                log.warn(
                        "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                        correlationId,
                        batch.topicPartition,
                        this.retries - batch.attempts() - 1,
                        formatErrMsg(response));
                reenqueueBatch(batch, now);
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number has advanced beyond
                // the sequence of the current batch, and we haven't retained batch metadata on the broker to return
                // the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid offset and timestamp.
                completeBatch(batch, response);
            } else {
                // tell the user the result of their request. We only adjust sequence numbers if the batch didn't exhaust
                // its retries -- if it did, we don't know whether the sequence number was accepted or not, and
                // thus it is not safe to reassign the sequence.
                failBatch(batch, response, batch.attempts() < this.retries);
            }
            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                                    "topic-partition may not exist or the user may not have Describe access to it",
                            batch.topicPartition);
                } else {
                    log.warn("Received invalid metadata error in produce request on partition {} due to {}. Going " +
                                    "to request metadata update now", batch.topicPartition,
                            error.exception(response.errorMessage).toString());
                }
                metadata.requestUpdate();
            }
        } else {
            completeBatch(batch, response);
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * Format the error from a {@link ProduceResponse.PartitionResponse} in a user-friendly string
     * e.g "NETWORK_EXCEPTION. Error Message: Disconnected from node 0"
     */
    private String formatErrMsg(ProduceResponse.PartitionResponse response) {
        String errorMessageSuffix = (response.errorMessage == null || response.errorMessage.isEmpty()) ?
                "" : String.format(". Error Message: %s", response.errorMessage);
        return String.format("%s%s", response.error, errorMessageSuffix);
    }

    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        this.accumulator.reenqueue(batch, currentTimeMs);
        maybeRemoveFromInflightBatches(batch);
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        if (transactionManager != null) {
            transactionManager.handleCompletedBatch(batch, response);
        }

        if (batch.complete(response.baseOffset, response.logAppendTime)) {
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    private void failBatch(ProducerBatch batch,
                           ProduceResponse.PartitionResponse response,
                           boolean adjustSequenceNumbers) {
        final RuntimeException topLevelException;
        if (response.error == Errors.TOPIC_AUTHORIZATION_FAILED)
            topLevelException = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
        else if (response.error == Errors.CLUSTER_AUTHORIZATION_FAILED)
            topLevelException = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
        else
            topLevelException = response.error.exception(response.errorMessage);

        if (response.recordErrors == null || response.recordErrors.isEmpty()) {
            failBatch(batch, topLevelException, adjustSequenceNumbers);
        } else {
            Map<Integer, RuntimeException> recordErrorMap = new HashMap<>(response.recordErrors.size());
            for (ProduceResponse.RecordError recordError : response.recordErrors) {
                // The API leaves us with some awkwardness interpreting the errors in the response.
                // We cannot differentiate between different error cases (such as INVALID_TIMESTAMP)
                // from the single error code at the partition level, so instead we use INVALID_RECORD
                // for all failed records and rely on the message to distinguish the cases.
                final String errorMessage;
                if (recordError.message != null) {
                    errorMessage = recordError.message;
                } else if (response.errorMessage != null) {
                    errorMessage = response.errorMessage;
                } else {
                    errorMessage = response.error.message();
                }

                // If the batch contained only a single record error, then we can unambiguously
                // use the exception type corresponding to the partition-level error code.
                if (response.recordErrors.size() == 1) {
                    recordErrorMap.put(recordError.batchIndex, response.error.exception(errorMessage));
                } else {
                    recordErrorMap.put(recordError.batchIndex, new InvalidRecordException(errorMessage));
                }
            }

            Function<Integer, RuntimeException> recordExceptions = batchIndex -> {
                RuntimeException exception = recordErrorMap.get(batchIndex);
                if (exception != null) {
                    return exception;
                } else {
                    // If the response contains record errors, then the records which failed validation
                    // will be present in the response. To avoid confusion for the remaining records, we
                    // return a generic exception.
                    return new KafkaException("Failed to append record because it was part of a batch " +
                            "which had one more more invalid records");
                }
            };

            failBatch(batch, topLevelException, recordExceptions, adjustSequenceNumbers);
        }
    }

    private void failBatch(
            ProducerBatch batch,
            RuntimeException topLevelException,
            boolean adjustSequenceNumbers
    ) {
        failBatch(batch, topLevelException, batchIndex -> topLevelException, adjustSequenceNumbers);
    }

    private void failBatch(
            ProducerBatch batch,
            RuntimeException topLevelException,
            Function<Integer, RuntimeException> recordExceptions,
            boolean adjustSequenceNumbers
    ) {
        if (transactionManager != null) {
            transactionManager.handleFailedBatch(batch, topLevelException, adjustSequenceNumbers);
        }

        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        if (batch.completeExceptionally(topLevelException, recordExceptions)) {
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed.
     * We can also retry OutOfOrderSequence exceptions for future batches, since if the first batch has failed, the
     * future batches are certain to fail with an OutOfOrderSequence exception.
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) &&
                batch.attempts() < this.retries &&
                !batch.isDone() &&
                (transactionManager == null ?
                        response.error.exception() instanceof RetriableException :
                        transactionManager.canRetry(response, batch));
    }

    /**
     * 将record batches批次转移到每个节点的生产请求列表中
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
    }

    /**
     * 从给定的记录批次创建生产请求
     *
     * @param now         当前时间戳
     * @param destination node节点
     * @param acks        ack确认机制
     * @param timeout     请求超时时间
     * @param batches     批次记录
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        if (batches.isEmpty())
            return;

        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());

        // 匹配不同的版本,版本向下兼容
        byte minUsedMagic = apiVersions.maxUsableProduceMagic();
        for (ProducerBatch batch : batches) {
            if (batch.magic() < minUsedMagic)
                minUsedMagic = batch.magic();
        }

        // 封装传输数据
        ProduceRequestData.TopicProduceDataCollection tpd = new ProduceRequestData.TopicProduceDataCollection();
        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;  // TopicPartition
            MemoryRecords records = batch.records();   // TopicPartition对应的记录

            // 消息未找到适配的版本,向下转换
            if (!records.hasMatchingMagic(minUsedMagic))
                records = batch.records().downConvert(minUsedMagic, 0, time).records();

            ProduceRequestData.TopicProduceData tpData = tpd.find(tp.topic());
            if (tpData == null) {
                tpData = new ProduceRequestData.TopicProduceData().setName(tp.topic());
                tpd.add(tpData);
            }
            tpData.partitionData().add(new ProduceRequestData.PartitionProduceData()
                    .setIndex(tp.partition())
                    .setRecords(records));
            recordsByPartition.put(tp, batch);
        }

        String transactionalId = null;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
        }

        // 生产者请求
        ProduceRequest.Builder requestBuilder = ProduceRequest.forMagic(minUsedMagic,
                new ProduceRequestData()
                        .setAcks(acks)
                        .setTimeoutMs(timeout)
                        .setTransactionalId(transactionalId)
                        .setTopicData(tpd));


        /* 回调处理 */
        RequestCompletionHandler callback =
                response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());

        String nodeId = Integer.toString(destination);

        ClientRequest clientRequest = client.newClientRequest(
                nodeId,
                requestBuilder,
                now,
                acks != 0,
                requestTimeoutMs,
                callback);

        // 真正的发送消息,排队等待
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    /**
     * 唤醒 NetworkClient 线程
     * wakeup() 的主要作用就是唤醒阻塞在 select()/select(long) 上的线程
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * A collection of sensors for the sender
     */
    private static class SenderMetrics {
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor batchSplitSensor;
        private final SenderMetricsRegistry metrics;
        private final Time time;

        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            this.metrics.addMetric(metrics.metadataAge,
                    (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (ProducerBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Objects.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // global metrics
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);
                    this.compressionRateSensor.record(batch.compressionRatio());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        void recordBatchSplit() {
            this.batchSplitSensor.record();
        }
    }

}
