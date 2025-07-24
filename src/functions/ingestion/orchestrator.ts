// C:\Users\SOHAM\Desktop\test godspeed\test-project\src\functions\ingestion\orchestrator.ts

import { IngestionData, IDestinationPlugin, IngestionDataTransformer, GSDataSource, IngestionEvents } from './interfaces';
import { GSStatus, logger, GSContext } from '@godspeedsystems/core';
// FIX 1: Corrected EventEmitter import syntax
import { EventEmitter } from 'events';

export class IngestionOrchestrator extends EventEmitter {
    private sourceDataSource: GSDataSource;
    private dataTransformer: IngestionDataTransformer;
    private destination: IDestinationPlugin | undefined;
    private taskId: string;
    private eventBus: EventEmitter;

    constructor(
        source: GSDataSource,
        transformer: IngestionDataTransformer,
        destination: IDestinationPlugin | undefined,
        eventBus: EventEmitter,
        taskId: string
    ) {
        super();
        this.sourceDataSource = source;
        this.dataTransformer = transformer;
        this.destination = destination;
        this.eventBus = eventBus;
        this.taskId = taskId;
        logger.info(`IngestionOrchestrator instance created for task ${this.taskId}.`);
    }

    public getEventBus(): EventEmitter {
        return this.eventBus;
    }

    async executeTask(ctx: GSContext, initialPayload?: any): Promise<GSStatus> {
        if (!this.sourceDataSource || !this.dataTransformer) {
            const errorMessage = "Orchestrator not fully configured. DataSource and dataTransformer are required.";
            logger.error(errorMessage);
            this.eventBus.emit(IngestionEvents.TASK_FAILED, this.taskId, { success: false, message: errorMessage });
            return new GSStatus(false, 400, errorMessage);
        }

        logger.info(`Starting ingestion task execution for task ${this.taskId}...`);
        let totalItemsProcessed = 0;

        try {
            logger.info(`Orchestrator: Initializing Godspeed DataSource client (${this.sourceDataSource.constructor.name}) for task ${this.taskId}...`);
            await this.sourceDataSource.initClient();
            logger.info(`Source client initialized for task ${this.taskId}.`);

            logger.info(`Orchestrator: Executing Godspeed DataSource (${this.sourceDataSource.constructor.name}) to fetch/process data...`);

            const sourceResultStatus: GSStatus = await this.sourceDataSource.execute(ctx);

            let rawData: any[] = [];
            if (sourceResultStatus.success) {
                if (sourceResultStatus.data && Array.isArray(sourceResultStatus.data.data)) {
                    rawData = sourceResultStatus.data.data;
                    logger.info(`Orchestrator: HttpCrawler yielded ${rawData.length} data items from 'status.data.data'.`);
                } else if (sourceResultStatus.data) {
                    rawData = [sourceResultStatus.data];
                    logger.info(`Orchestrator: GitCrawler yielded 1 data item from 'status.data'.`);
                } else {
                    logger.warn(`Orchestrator: Source executed successfully but returned no data in 'status.data' for task ${this.taskId}.`);
                }
            } else {
                const errorMessage = `Source execution failed for task ${this.taskId}: ${sourceResultStatus.message}`;
                logger.error(errorMessage, { data: sourceResultStatus.data });
                this.eventBus.emit(IngestionEvents.TASK_FAILED, this.taskId, { success: false, message: errorMessage, data: sourceResultStatus.data });
                return new GSStatus(false, 500, errorMessage, { data: sourceResultStatus.data });
            }

            this.eventBus.emit(IngestionEvents.DATA_FETCHED, rawData, this.taskId);
            logger.info(`Orchestrator: Prepared ${rawData.length} raw data items for transformation.`);

            const transformedData: IngestionData[] = await this.dataTransformer(rawData, initialPayload);

            this.eventBus.emit(IngestionEvents.DATA_TRANSFORMED, transformedData, this.taskId);
            logger.info(`Orchestrator: Transformed data, received ${transformedData.length} data items.`);

            if (transformedData.length === 0) {
                logger.warn(`Orchestrator: No data ingested from source for task ${this.taskId}. Task completed with no data.`);
                const status = new GSStatus(true, 200, "Ingestion task completed: No data from source.", { itemsProcessed: 0 });
                this.eventBus.emit(IngestionEvents.TASK_COMPLETED, this.taskId, status);
                return status;
            }

            logger.info(`Orchestrator: Processing data for destination (if configured) for task ${this.taskId}...`);

            if (this.destination) {
                try {
                    const sendResult = await this.destination.processData(transformedData);

                    if (!sendResult.success) {
                        logger.error(`Orchestrator: Destination processing failed for task ${this.taskId}: ${sendResult.message}`, { data: sendResult.data });
                        const failureStatus = new GSStatus(false, 500, `Destination processing failed for task ${this.taskId}: ${sendResult.message}`, { itemsProcessed: totalItemsProcessed, data: sendResult.data });
                        this.eventBus.emit(IngestionEvents.TASK_FAILED, this.taskId, failureStatus);
                        return failureStatus;
                    } else {
                        totalItemsProcessed = transformedData.length;
                        this.eventBus.emit(IngestionEvents.DATA_PROCESSED, transformedData, this.taskId);
                        logger.info(`Orchestrator: Destination processing complete for task ${this.taskId}.`);
                    }
                } catch (sendError: any) {
                    logger.error(`Orchestrator: Error during destination processing for task ${this.taskId}: ${sendError.message}`, { error: sendError });
                    const failureStatus = new GSStatus(false, 500, `Error during destination processing for task ${this.taskId}: ${sendError.message}`, { itemsProcessed: totalItemsProcessed, data: sendError.message });
                    this.eventBus.emit(IngestionEvents.TASK_FAILED, this.taskId, failureStatus);
                    return failureStatus;
                }
            } else {
                totalItemsProcessed = transformedData.length;
                logger.info(`Orchestrator: No destination configured for task ${this.taskId}. Data considered processed after transformation.`);
            }

            logger.info(`Ingestion task ${this.taskId} completed. Total items processed/emitted: ${totalItemsProcessed}.`);
            const successStatus = new GSStatus(true, 200, "Ingestion task completed successfully.", { itemsProcessed: totalItemsProcessed });
            this.eventBus.emit(IngestionEvents.TASK_COMPLETED, this.taskId, successStatus);
            return successStatus;

        } catch (error: any) {
            const errorMessage = `Ingestion task ${this.taskId} failed: ${error.message}`;
            logger.error(errorMessage, { error: error });
            const failureStatus = new GSStatus(false, 500, errorMessage, { itemsProcessed: totalItemsProcessed, data: error.message });
            this.eventBus.emit(IngestionEvents.TASK_FAILED, this.taskId, failureStatus);
            return failureStatus;
        }
    }
}
