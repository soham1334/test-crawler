// C:\Users\SOHAM\Desktop\crawler\test-crawler\src\functions\ingestion\interfaces.ts

import { GSStatus, GSContext } from '@godspeedsystems/core';
import { EventEmitter } from 'events';

// --- Ingestion Data Structures ---
export interface IngestionData {
    id: string; // Unique identifier for the data item
    content: string | Buffer | object;
    metadata?: { // Optional metadata associated with the data
        [key: string]: any;
        filename?: string; // e.g., original filename
        relativePath?: string; // e.g., path within a source system
    };
    fetchedAt?: Date;
    url?: string;     // Added for HTTP Crawler output
    statusCode?: number; // Added for HTTP Crawler output
    [key: string]: any; // Allows for additional arbitrary properties
}

export type IngestionDataTransformer = (rawData: any[], initialPayload?: any) => Promise<IngestionData[]>;


// --- Plugin Interfaces ---

export interface GSDataSource {
    // FIX: Modified the execute method signature to accept initialPayload
    execute(ctx: GSContext, initialPayload?: any): Promise<GSStatus>;

    initClient(): Promise<any>;

    init?(config: any): Promise<void>;
}

export interface IDestinationPlugin {
    init(config: any): Promise<void>; // Initialize with destination-specific config
    processData(data: IngestionData[]): Promise<GSStatus>; // Process a batch of ingested data
}


// --- Task Definition Interfaces ---

export type TriggerType = 'cron' | 'webhook' | 'manual';

export interface BaseTrigger {
    type: TriggerType;
    enabled?: boolean;
}

export interface CronTrigger extends BaseTrigger {
    type: 'cron';
    expression: string; // Cron expression, e.g., "0 0 * * *"
}

export interface WebhookTrigger extends BaseTrigger {
    type: 'webhook';
    endpointId: string; // A unique ID for the webhook endpoint
}

export interface ManualTrigger extends BaseTrigger {
    type: 'manual';
    // No specific properties needed for manual trigger other than 'type'
}

export type IngestionTrigger = CronTrigger | WebhookTrigger | ManualTrigger;

export enum IngestionTaskStatus {
    SCHEDULED = 'SCHEDULED',
    RUNNING = 'RUNNING',
    COMPLETED = 'COMPLETED',
    FAILED = 'FAILED',
    DISABLED = 'DISABLED',
}

export interface IngestionTaskDefinition {
    id: string; // Unique ID for the ingestion task (UUID)
    name: string; // Human-readable name
    description?: string;
    enabled: boolean; // Whether the task is active or not
    trigger: IngestionTrigger;
    source: {
        pluginType: string; // e.g., 's3', 'ftp', 'api'
        config: any; // Source-specific configuration
    };
    destination?: { // Destination is optional (e.g., if only transforming data)
        pluginType: string; // e.g., 'filesystem', 'database', 'sftp'
        config: any; // Destination-specific configuration
    };
    // Optional additional properties for task management
    currentStatus?: IngestionTaskStatus;
    lastRun?: Date;
    lastRunStatus?: GSStatus;
    nextRun?: Date; // For cron jobs, calculated next run time
    // Any custom parameters or configurations for the transformer function itself
    transformerParams?: any;
}


// --- Lifecycle Manager Interface ---
export interface IGlobalIngestionLifecycleManager {
    init(): Promise<void>;
    start(): Promise<void>;
    stop(): Promise<void>;
    registerSource(pluginType: string, sourcePlugin: new (...args: any[]) => GSDataSource, transformer: IngestionDataTransformer): void;
    registerDestination(pluginType: string, destinationPlugin: new (...args: any[]) => IDestinationPlugin): void;
    scheduleTask(taskDefinition: IngestionTaskDefinition): Promise<GSStatus>;
    updateTask(taskId: string, updates: Partial<IngestionTaskDefinition>): Promise<GSStatus>;
    enableTask(taskId: string): Promise<GSStatus>;
    disableTask(taskId: string): Promise<GSStatus>;
    deleteTask(taskId: string): Promise<GSStatus>;
    getTask(taskId: string): IngestionTaskDefinition | undefined;
    listTasks(): IngestionTaskDefinition[];
    triggerManualTask(ctx: GSContext,taskId: string, initialPayload?: any): Promise<GSStatus>;
    triggerWebhookTask(ctx: GSContext,endpointId: string, payload: any): Promise<GSStatus>;
    triggerAllEnabledCronTasks(ctx: GSContext): Promise<GSStatus>;
    getEventBus(): EventEmitter;
}


// --- Orchestrator Interface (Optional, but good for explicit typing if used elsewhere) ---
export interface IIngestionOrchestrator {
    executeTask(ctx: GSContext,initialPayload?: any): Promise<GSStatus>;
    getEventBus(): EventEmitter;
}


// --- Events for Ingestion Lifecycle ---
export const IngestionEvents = {
    TASK_SCHEDULED: 'task_scheduled',
    TASK_UPDATED: 'task_updated',
    TASK_ENABLED: 'task_enabled',
    TASK_DISABLED: 'task_disabled',
    TASK_DELETED: 'task_deleted',
    TASK_STARTED: 'task_started',
    TASK_TRIGGERED: 'task_triggered',
    TASK_COMPLETED: 'task_completed',
    TASK_FAILED: 'task_failed',
    DATA_FETCHED: 'data_fetched',
    DATA_TRANSFORMED: 'data_transformed',
    DATA_PROCESSED: 'data_processed',
    // Add more granular events as needed
};
