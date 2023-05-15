/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.common.broadcast.operator;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheSnapshot;
import org.apache.flink.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.iteration.datacache.nonkeyed.Segment;
import org.apache.flink.iteration.operator.OperatorUtils;
import org.apache.flink.iteration.proxy.state.ProxyStreamOperatorStateContext;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.ml.common.broadcast.BroadcastContext;
import org.apache.flink.ml.common.broadcast.BroadcastStreamingRuntimeContext;
import org.apache.flink.ml.common.broadcast.typeinfo.CacheElement;
import org.apache.flink.ml.common.broadcast.typeinfo.CacheElementSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.util.NonClosingInputStreamDecorator;
import org.apache.flink.runtime.util.NonClosingOutputStreamDecorator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.InputConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.NetworkInputConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.CheckpointedStreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.commons.collections.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Base class for the broadcast wrapper operators.
 *
 * <p>Note that not all instances of {@link AbstractBroadcastWrapperOperator} need to access the
 * broadcast variables. If one instance of {@link AbstractBroadcastWrapperOperator} does not contain
 * a rich function, then it can directly process the input without waiting for the broadcast
 * variables.
 */
public abstract class AbstractBroadcastWrapperOperator<T, S extends StreamOperator<T>>
        implements StreamOperator<T>, StreamOperatorStateHandler.CheckpointedStreamOperator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractBroadcastWrapperOperator.class);

    protected final StreamOperatorParameters<T> parameters;

    protected final StreamConfig streamConfig;

    protected final StreamTask<?, ?> containingTask;

    protected final Output<StreamRecord<T>> output;

    protected final StreamOperatorFactory<T> operatorFactory;

    protected final OperatorMetricGroup metrics;

    protected final S wrappedOperator;

    protected transient StreamOperatorStateHandler stateHandler;

    protected transient InternalTimeServiceManager<?> timeServiceManager;

    // ---------------- context info for rich function ----------------
    private MailboxExecutor mailboxExecutor;

    private String[] broadcastStreamNames;

    /**
     * Whether each input is blocked. Inputs with broadcast variables can only process their input
     * records after broadcast variables are ready. One input is non-blocked if it can consume its
     * inputs (by caching) when broadcast variables are not ready. Otherwise it has to block the
     * processing and wait until the broadcast variables are ready to be accessed.
     */
    private boolean[] isBlocked;

    /** Type serializer of each input. */
    private TypeSerializer<?>[] inTypeSerializers;

    /** Whether all broadcast variables of this operator are ready. */
    private boolean broadcastVariablesReady;
    /** Index of this subtask. */
    protected transient int indexOfSubtask;

    /** Number of the inputs of this operator. */
    protected int numInputs;

    /** RuntimeContext of the rich function in wrapped operator. */
    private BroadcastStreamingRuntimeContext wrappedOperatorRuntimeContext;

    /**
     * Path of the file used to store the cached records. It could be local file system or remote
     * file system.
     */
    private Path basePath;

    /** DataCacheWriter for each input. */
    @SuppressWarnings("rawtypes")
    private DataCacheWriter[] dataCacheWriters;

    /** Whether each input has pending elements. */
    private boolean[] hasPendingElements;

    /**
     * Whether this operator has a rich function and needs to access the broadcast variable. If yes,
     * it cannot process elements until the broadcast variables are ready.
     */
    private final boolean hasRichFunction;

    int numCachedRecords = 0;

    @SuppressWarnings({"unchecked", "rawtypes"})
    AbstractBroadcastWrapperOperator(
            StreamOperatorParameters<T> parameters,
            StreamOperatorFactory<T> operatorFactory,
            String[] broadcastStreamNames) {
        this.parameters = Objects.requireNonNull(parameters);
        this.streamConfig = Objects.requireNonNull(parameters.getStreamConfig());
        this.containingTask = Objects.requireNonNull(parameters.getContainingTask());
        this.output = Objects.requireNonNull(parameters.getOutput());
        this.operatorFactory = Objects.requireNonNull(operatorFactory);
        this.metrics = createOperatorMetricGroup(containingTask.getEnvironment(), streamConfig);
        this.wrappedOperator =
                (S)
                        StreamOperatorFactoryUtil.<T, S>createOperator(
                                        operatorFactory,
                                        (StreamTask) containingTask,
                                        streamConfig,
                                        output,
                                        parameters.getOperatorEventDispatcher())
                                .f0;

        this.hasRichFunction =
                wrappedOperator instanceof AbstractUdfStreamOperator
                        && ((AbstractUdfStreamOperator) wrappedOperator).getUserFunction()
                                instanceof RichFunction;

        if (hasRichFunction) {
            this.wrappedOperatorRuntimeContext =
                    new BroadcastStreamingRuntimeContext(
                            containingTask.getEnvironment(),
                            containingTask.getEnvironment().getAccumulatorRegistry().getUserMap(),
                            wrappedOperator.getMetricGroup(),
                            wrappedOperator.getOperatorID(),
                            ((AbstractUdfStreamOperator) wrappedOperator)
                                    .getProcessingTimeService(),
                            null,
                            containingTask.getEnvironment().getExternalResourceInfoProvider());

            ((RichFunction) ((AbstractUdfStreamOperator) wrappedOperator).getUserFunction())
                    .setRuntimeContext(wrappedOperatorRuntimeContext);

            this.mailboxExecutor =
                    containingTask
                            .getMailboxExecutorFactory()
                            .createExecutor(TaskMailbox.MIN_PRIORITY);

            this.indexOfSubtask = containingTask.getIndexInSubtaskGroup();

            // Puts in mailboxExecutor.
            for (String name : broadcastStreamNames) {
                BroadcastContext.putMailBoxExecutor(name + "-" + indexOfSubtask, mailboxExecutor);
            }

            this.broadcastStreamNames = broadcastStreamNames;

            InputConfig[] inputConfigs =
                    streamConfig.getInputs(containingTask.getUserCodeClassLoader());

            int numNetworkInputs = 0;
            while (numNetworkInputs < inputConfigs.length
                    && inputConfigs[numNetworkInputs] instanceof NetworkInputConfig) {
                numNetworkInputs++;
            }
            this.numInputs = numNetworkInputs;

            this.isBlocked = new boolean[numInputs];
            Arrays.fill(isBlocked, false);

            this.inTypeSerializers = new TypeSerializer[numInputs];
            for (int i = 0; i < numInputs; i++) {
                inTypeSerializers[i] =
                        streamConfig.getTypeSerializerIn(
                                i, containingTask.getUserCodeClassLoader());
            }

            this.broadcastVariablesReady = false;

            this.basePath =
                    OperatorUtils.getDataCachePath(
                            containingTask.getEnvironment().getTaskManagerInfo().getConfiguration(),
                            containingTask
                                    .getEnvironment()
                                    .getIOManager()
                                    .getSpillingDirectoriesPaths());
            this.dataCacheWriters = new DataCacheWriter[numInputs];
            this.hasPendingElements = new boolean[numInputs];
            Arrays.fill(hasPendingElements, true);
        }
    }

    /**
     * Checks whether all broadcast variables are ready. Besides, it maintains a state
     * {broadcastVariablesReady} to avoiding invoking {@code BroadcastContext.isCacheFinished(...)}
     * repeatedly. Finally, it sets broadcast variables for {wrappedOperatorRuntimeContext} if the
     * broadcast variables are ready.
     *
     * @return true if all broadcast variables are ready, false otherwise.
     */
    protected boolean areBroadcastVariablesReady() {
        // for debug
        int numCachedRecordsBeforeProcessing = 5;
        if (broadcastVariablesReady && numCachedRecords >= numCachedRecordsBeforeProcessing) {
            return true;
        }
        for (String name : broadcastStreamNames) {
            if (!BroadcastContext.isCacheFinished(name + "-" + indexOfSubtask)) {
                return false;
            } else {
                String key = name + "-" + indexOfSubtask;
                String userKey = name.substring(name.indexOf('-') + 1);
                wrappedOperatorRuntimeContext.setBroadcastVariable(
                        userKey, BroadcastContext.getBroadcastVariable(key));
            }
        }
        broadcastVariablesReady = true;
        return numCachedRecords > numCachedRecordsBeforeProcessing;
    }

    private OperatorMetricGroup createOperatorMetricGroup(
            Environment environment, StreamConfig streamConfig) {
        try {
            OperatorMetricGroup operatorMetricGroup =
                    environment
                            .getMetricGroup()
                            .getOrAddOperator(
                                    streamConfig.getOperatorID(), streamConfig.getOperatorName());
            if (streamConfig.isChainEnd()) {
                ((InternalOperatorIOMetricGroup) operatorMetricGroup.getIOMetricGroup())
                        .reuseOutputMetricsForTask();
            }
            return operatorMetricGroup;
        } catch (Exception e) {
            LOG.warn("An error occurred while instantiating task metrics.", e);
            return UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        }
    }

    /**
     * Extracts common processing logic in subclasses' processing elements.
     *
     * @param streamRecord the input record.
     * @param inputIndex input id, starts from zero.
     * @param elementConsumer the consumer function of StreamRecord, i.e.,
     *     operator.processElement(...).
     * @param watermarkConsumer the consumer function of WaterMark, i.e.,
     *     operator.processWatermark(...).
     * @param keyContextSetter the consumer function of setting key context, i.e.,
     *     operator.setKeyContext(...).
     * @throws Exception possible exception.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void processElementX(
            StreamRecord streamRecord,
            int inputIndex,
            ThrowingConsumer<StreamRecord, Exception> elementConsumer,
            ThrowingConsumer<Watermark, Exception> watermarkConsumer,
            ThrowingConsumer<StreamRecord, Exception> keyContextSetter)
            throws Exception {
        // we first process the pending mail
        if (!hasRichFunction) {
            elementConsumer.accept(streamRecord);
        } else if (isBlocked[inputIndex]) {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            elementConsumer.accept(streamRecord);
        } else if (!areBroadcastVariablesReady()) {
            dataCacheWriters[inputIndex].addRecord(CacheElement.newRecord(streamRecord.getValue()));
            numCachedRecords++;
        } else {
            if (hasPendingElements[inputIndex]) {
                boolean hasRemaining =
                        processPendingElementsAndWatermarks(
                                inputIndex,
                                elementConsumer,
                                watermarkConsumer,
                                keyContextSetter,
                                3);
                hasPendingElements[inputIndex] = hasRemaining;
            }
            if (hasPendingElements[inputIndex]) {
                dataCacheWriters[inputIndex].addRecord(
                        CacheElement.newRecord(streamRecord.getValue()));
            } else {
                keyContextSetter.accept(streamRecord);
                elementConsumer.accept(streamRecord);
            }
        }
    }

    /**
     * Extracts common processing logic in subclasses' processing watermarks.
     *
     * @param watermark the input watermark.
     * @param inputIndex input id, starts from zero.
     * @param elementConsumer the consumer function of StreamRecord, i.e.,
     *     operator.processElement(...).
     * @param watermarkConsumer the consumer function of WaterMark, i.e.,
     *     operator.processWatermark(...).
     * @param keyContextSetter the consumer function of setting key context, i.e.,
     *     operator.setKeyContext(...).
     * @throws Exception possible exception.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void processWatermarkX(
            Watermark watermark,
            int inputIndex,
            ThrowingConsumer<StreamRecord, Exception> elementConsumer,
            ThrowingConsumer<Watermark, Exception> watermarkConsumer,
            ThrowingConsumer<StreamRecord, Exception> keyContextSetter)
            throws Exception {
        if (!hasRichFunction) {
            watermarkConsumer.accept(watermark);
        } else if (isBlocked[inputIndex]) {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            watermarkConsumer.accept(watermark);
        } else if (!areBroadcastVariablesReady()) {
            dataCacheWriters[inputIndex].addRecord(
                    CacheElement.newWatermark(watermark.getTimestamp()));
        } else {
            if (hasPendingElements[inputIndex]) {
                boolean hasRemaining =
                        processPendingElementsAndWatermarks(
                                inputIndex,
                                elementConsumer,
                                watermarkConsumer,
                                keyContextSetter,
                                3);
                hasPendingElements[inputIndex] = hasRemaining;
            }

            if (!hasPendingElements[inputIndex]) {
                dataCacheWriters[inputIndex].addRecord(
                        CacheElement.newWatermark(watermark.getTimestamp()));
            } else {
                watermarkConsumer.accept(watermark);
            }
        }
    }

    /**
     * Extracts common processing logic in subclasses' endInput(...).
     *
     * @param inputIndex input id, starts from zero.
     * @param elementConsumer the consumer function of StreamRecord, i.e.,
     *     operator.processElement(...).
     * @param watermarkConsumer the consumer function of WaterMark, i.e.,
     *     operator.processWatermark(...).
     * @param keyContextSetter the consumer function of setting key context, i.e.,
     *     operator.setKeyContext(...).
     * @throws Exception possible exception.
     */
    @SuppressWarnings("rawtypes")
    protected void endInputX(
            int inputIndex,
            ThrowingConsumer<StreamRecord, Exception> elementConsumer,
            ThrowingConsumer<Watermark, Exception> watermarkConsumer,
            ThrowingConsumer<StreamRecord, Exception> keyContextSetter)
            throws Exception {
        if (!hasRichFunction) {
            return;
        }
        if (hasPendingElements[inputIndex]) {
            processPendingElementsAndWatermarks(
                    inputIndex,
                    elementConsumer,
                    watermarkConsumer,
                    keyContextSetter,
                    Long.MAX_VALUE);
            hasPendingElements[inputIndex] = false;
        }
    }

    /**
     * Processes the pending elements that are cached by {@link DataCacheWriter}.
     *
     * @param inputIndex input id, starts from zero.
     * @param elementConsumer the consumer function of StreamRecord, i.e.,
     *     operator.processElement(...).
     * @param watermarkConsumer the consumer function of WaterMark, i.e.,
     *     operator.processWatermark(...).
     * @param keyContextSetter the consumer function of setting key context, i.e.,
     *     operator.setKeyContext(...). return True if has remaining data.
     * @throws Exception possible exception.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean processPendingElementsAndWatermarks(
            int inputIndex,
            ThrowingConsumer<StreamRecord, Exception> elementConsumer,
            ThrowingConsumer<Watermark, Exception> watermarkConsumer,
            ThrowingConsumer<StreamRecord, Exception> keyContextSetter,
            long batchNum)
            throws Exception {
        List<Segment> pendingSegments = dataCacheWriters[inputIndex].getSegments();

        int numCachedRecordsProcessed = 0;
        if (pendingSegments.size() != 0) {

            List<Segment> toRead = new ArrayList<>();

            for (int i = 0; i < Math.min(batchNum, pendingSegments.size()); i++) {
                toRead.add(pendingSegments.get(i));
            }

            DataCacheReader dataCacheReader =
                    new DataCacheReader<>(
                            new CacheElementSerializer<>(inTypeSerializers[inputIndex]), toRead);
            int num = 0;
            while (dataCacheReader.hasNext() && num < batchNum) {
                CacheElement cacheElement = (CacheElement) dataCacheReader.next();
                switch (cacheElement.getType()) {
                    case RECORD:
                        StreamRecord record = new StreamRecord(cacheElement.getRecord());
                        keyContextSetter.accept(record);
                        elementConsumer.accept(record);
                        numCachedRecordsProcessed++;
                        System.out.println(
                                "processed cached records, cnt: "
                                        + numCachedRecordsProcessed
                                        + " at time: "
                                        + System.currentTimeMillis());
                        num++;
                        break;
                    case WATERMARK:
                        watermarkConsumer.accept(new Watermark(cacheElement.getWatermark()));
                        break;
                    default:
                        throw new RuntimeException(
                                "Unsupported CacheElement type: " + cacheElement.getType());
                }
            }
            for (int i = 0; i < Math.min(batchNum, pendingSegments.size()); i++) {
                dataCacheWriters[inputIndex].remove(i);
            }

            //            if (pendingSegments.isEmpty()) {
            //                dataCacheWriters[inputIndex].clear();
            //            }
            return pendingSegments.size() > 0;
        }
        return false;
    }

    @Override
    public void open() throws Exception {
        wrappedOperator.open();
    }

    @Override
    public void close() throws Exception {
        wrappedOperator.close();
        if (!hasRichFunction) {
            return;
        }
        for (String name : broadcastStreamNames) {
            BroadcastContext.remove(name + "-" + indexOfSubtask);
        }
    }

    @Override
    public void finish() throws Exception {
        wrappedOperator.finish();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        wrappedOperator.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        final TypeSerializer<?> keySerializer =
                streamConfig.getStateKeySerializer(containingTask.getUserCodeClassLoader());

        StreamOperatorStateContext streamOperatorStateContext =
                streamTaskStateManager.streamOperatorStateContext(
                        getOperatorID(),
                        getClass().getSimpleName(),
                        parameters.getProcessingTimeService(),
                        this,
                        keySerializer,
                        containingTask.getCancelables(),
                        metrics,
                        streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.STATE_BACKEND,
                                containingTask
                                        .getEnvironment()
                                        .getTaskManagerInfo()
                                        .getConfiguration(),
                                containingTask.getUserCodeClassLoader()),
                        false);
        stateHandler =
                new StreamOperatorStateHandler(
                        streamOperatorStateContext,
                        containingTask.getExecutionConfig(),
                        containingTask.getCancelables());
        stateHandler.initializeOperatorState(this);

        timeServiceManager = streamOperatorStateContext.internalTimerServiceManager();

        wrappedOperator.initializeState(
                (operatorID,
                        operatorClassName,
                        processingTimeService,
                        keyContext,
                        keySerializerX,
                        streamTaskCloseableRegistry,
                        metricGroup,
                        managedMemoryFraction,
                        isUsingCustomRawKeyedState) ->
                        new ProxyStreamOperatorStateContext(
                                streamOperatorStateContext,
                                "wrapped-",
                                CloseableIterator.empty(),
                                0));
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        return stateHandler.snapshotState(
                this,
                Optional.ofNullable(timeServiceManager),
                streamConfig.getOperatorName(),
                checkpointId,
                timestamp,
                checkpointOptions,
                storageLocation,
                false);
    }

    @Override
    @SuppressWarnings("unchecked, rawtypes")
    public void initializeState(StateInitializationContext stateInitializationContext)
            throws Exception {
        List<StatePartitionStreamProvider> inputs =
                IteratorUtils.toList(
                        stateInitializationContext.getRawOperatorStateInputs().iterator());
        Preconditions.checkState(
                inputs.size() < 2, "The input from raw operator state should be one or zero.");
        if (!hasRichFunction) {
            return;
        }
        if (inputs.size() == 0) {
            for (int i = 0; i < numInputs; i++) {
                dataCacheWriters[i] =
                        new DataCacheWriter(
                                new CacheElementSerializer(inTypeSerializers[i]),
                                basePath.getFileSystem(),
                                OperatorUtils.createDataCacheFileGenerator(
                                        basePath, "cache", streamConfig.getOperatorID()));
            }
        } else {
            InputStream inputStream = inputs.get(0).getStream();
            DataInputStream dis =
                    new DataInputStream(new NonClosingInputStreamDecorator(inputStream));
            Preconditions.checkState(dis.readInt() == numInputs, "Number of input is wrong.");
            for (int i = 0; i < numInputs; i++) {
                DataCacheSnapshot dataCacheSnapshot =
                        DataCacheSnapshot.recover(
                                inputStream,
                                basePath.getFileSystem(),
                                OperatorUtils.createDataCacheFileGenerator(
                                        basePath, "cache", streamConfig.getOperatorID()));
                dataCacheWriters[i] =
                        new DataCacheWriter(
                                new CacheElementSerializer(inTypeSerializers[i]),
                                basePath.getFileSystem(),
                                OperatorUtils.createDataCacheFileGenerator(
                                        basePath, "cache", streamConfig.getOperatorID()),
                                dataCacheSnapshot.getSegments());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void snapshotState(StateSnapshotContext stateSnapshotContext) throws Exception {
        System.out.println(
                getClass().getSimpleName()
                        + " doing checkpoint with checkpoint id: "
                        + stateSnapshotContext.getCheckpointId()
                        + " at time: "
                        + System.currentTimeMillis());
        if (wrappedOperator instanceof StreamOperatorStateHandler.CheckpointedStreamOperator) {
            ((CheckpointedStreamOperator) wrappedOperator).snapshotState(stateSnapshotContext);
        }

        if (!hasRichFunction) {
            return;
        }

        OperatorStateCheckpointOutputStream checkpointOutputStream =
                stateSnapshotContext.getRawOperatorStateOutput();
        checkpointOutputStream.startNewPartition();
        try (DataOutputStream dos =
                new DataOutputStream(new NonClosingOutputStreamDecorator(checkpointOutputStream))) {
            dos.writeInt(numInputs);
        }
        for (int i = 0; i < numInputs; i++) {
            dataCacheWriters[i].writeSegmentsToFiles();
            DataCacheSnapshot dataCacheSnapshot =
                    new DataCacheSnapshot(
                            basePath.getFileSystem(), null, dataCacheWriters[i].getSegments());
            dataCacheSnapshot.writeTo(checkpointOutputStream);
        }
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        wrappedOperator.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        wrappedOperator.setKeyContextElement2(record);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return wrappedOperator.getMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        return wrappedOperator.getOperatorID();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        wrappedOperator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        wrappedOperator.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void setCurrentKey(Object key) {
        wrappedOperator.setCurrentKey(key);
    }

    @Override
    public Object getCurrentKey() {
        return wrappedOperator.getCurrentKey();
    }
}
