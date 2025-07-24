// C:\Users\SOHAM\Desktop\test godspeed\test-project\src\test/GlobalIngestionLifecycleManager.ts

import {
    IGlobalIngestionLifecycleManager,
    IngestionTaskDefinition,
    IngestionTask,
    TaskStatus,
    IngestionDataTransformer,
    IDestinationPlugin,
    IngestionData // Import IngestionData for event typing
} from './interfaces'; // Adjusted path as per your provided error messages, assuming interfaces.ts is at the root of 'test'
import { IngestionOrchestrator, IngestionEvents } from './orchestrator'; // Adjusted path, assuming orchestrator.ts is at the root of 'test'
import { GSDataSource, GSStatus, logger } from '@godspeedsystems/core';
import { randomUUID } from 'crypto';
import { EventEmitter } from 'events'; // Import EventEmitter

// Define a type for a Godspeed DataSource constructor with initClient
type DataSourceConstructor = new (options: { config: any }) => GSDataSource & { initClient?: () => Promise<object> };
// Define a type for a Destination Plugin constructor
type DestinationConstructor = new () => IDestinationPlugin;

export class GlobalIngestionLifecycleManager implements IGlobalIngestionLifecycleManager {
    private tasks: Map<string, IngestionTask>;
    private sourceRegistry: Map<string, { constructor: DataSourceConstructor, transformer: IngestionDataTransformer }>;
    private destinationRegistry: Map<string, DestinationConstructor>;
    private eventBus: EventEmitter; // Manager's central event bus
    private lifecycleStarted: boolean = false; // Internal flag to track if start() has been called

    constructor() {
        this.tasks = new Map();
        this.sourceRegistry = new Map();
        this.destinationRegistry = new Map();
        this.eventBus = new EventEmitter(); // Initialize the event bus
        logger.info("GlobalIngestionLifecycleManager initialized.");
    }

    /**
     * Exposes the manager's central event bus.
     * Consumers can subscribe to events like DATA_RECEIVED, TASK_COMPLETED, TASK_FAILED.
     * @returns The EventEmitter instance for the manager.
     */
    public getEventBus(): EventEmitter {
        return this.eventBus;
    }

    /**
     * Registers a data source plugin and its associated transformer function.
     * @param pluginType A unique string identifier for the source (e.g., "http-crawler").
     * @param dataSourceConstructor The constructor of the Godspeed DataSource.
     * @param transformer The function to transform GSDataSource output into IngestionData.
     */
    registerSource(
        pluginType: string,
        dataSourceConstructor: new (options: { config: any }) => GSDataSource & { initClient?: () => Promise<object> },
        transformer: IngestionDataTransformer
    ): void {
        if (this.sourceRegistry.has(pluginType)) {
            logger.warn(`Source plugin "${pluginType}" is already registered. Overwriting.`);
        }
        this.sourceRegistry.set(pluginType, { constructor: dataSourceConstructor, transformer });
        logger.info(`Source plugin "${pluginType}" registered.`);
    }

    /**
     * Registers a destination plugin.
     * @param pluginType A unique string identifier for the destination (e.g., "api-destination", "file-system-saver").
     * @param destinationConstructor The constructor of the IDestinationPlugin implementation.
     */
    registerDestination(
        pluginType: string,
        destinationConstructor: new () => IDestinationPlugin
    ): void {
        if (this.destinationRegistry.has(pluginType)) {
            logger.warn(`Destination plugin "${pluginType}" is already registered. Overwriting.`);
        }
        this.destinationRegistry.set(pluginType, destinationConstructor);
        logger.info(`Destination plugin "${pluginType}" registered.`);
    }

    /**
     * Schedules a new ingestion task.
     * @param taskDef The definition of the task.
     * @returns The scheduled IngestionTask object, including its assigned ID.
     * @throws If a source or destination plugin is not registered.
     */
    async scheduleTask(taskDef: IngestionTaskDefinition): Promise<IngestionTask> {
        // Ensure taskId is always a string. If not provided, generate one.
        const taskId: string = taskDef.id || randomUUID();
        const existingTask = this.tasks.get(taskId);

        if (existingTask) {
            throw new Error(`Task with ID "${taskId}" already exists. Use updateTask to modify.`);
        }

        if (!this.sourceRegistry.has(taskDef.source.pluginType)) {
            throw new Error(`Cannot schedule task "${taskDef.name}": Source plugin "${taskDef.source.pluginType}" not registered.`);
        }

        if (taskDef.destination && !this.destinationRegistry.has(taskDef.destination.pluginType)) {
            throw new Error(`Cannot schedule task "${taskDef.name}": Destination plugin "${taskDef.destination.pluginType}" not registered.`);
        }

        const newTask: IngestionTask = {
            ...taskDef,
            id: taskId, // Assign the guaranteed string taskId
            currentStatus: 'scheduled',
            enabled: taskDef.enabled !== undefined ? taskDef.enabled : true // Default to true if not specified
        };

        this.tasks.set(taskId, newTask);
        logger.info(`Task "${newTask.name}" (ID: ${taskId}) scheduled.`);

        // Setup trigger immediately if manager is already running
        if (this.lifecycleStarted) {
            this._setupTrigger(newTask);
        }

        return newTask;
    }

    /**
     * Deschedules an existing ingestion task.
     * For cron tasks, clears the interval.
     * @param taskId The ID of the task to deschedule.
     * @returns True if descheduled, false if not found.
     */
    async descheduleTask(taskId: string): Promise<boolean> {
        const task = this.tasks.get(taskId);
        if (!task) {
            logger.warn(`Task ${taskId} not found, cannot deschedule.`);
            return false;
        }

        if (task.trigger.type === 'cron' && task.trigger._intervalId) {
            clearInterval(task.trigger._intervalId);
            delete task.trigger._intervalId;
            logger.info(`Cron interval cleared for task ${task.id}.`); // task.id is guaranteed string here
        }

        task.currentStatus = 'descheduled';
        logger.info(`Task "${task.name}" (ID: ${task.id}) descheduled.`); // task.id is guaranteed string here
        return true;
    }

    /**
     * Updates an existing ingestion task.
     * @param taskId The ID of the task to update.
     * @param updates Partial updates to the task definition.
     * @returns The updated IngestionTask object.
     * @throws If the task is not found.
     */
    async updateTask(taskId: string, updates: Partial<IngestionTaskDefinition>): Promise<IngestionTask> {
        const task = this.tasks.get(taskId);
        if (!task) {
            throw new Error(`Task with ID "${taskId}" not found.`);
        }

        const originalEnabled = task.enabled;
        const originalTriggerType = task.trigger.type; // For trigger re-setup

        Object.assign(task, updates); // Apply partial updates

        // Re-validate source/destination plugins if they were updated
        if (updates.source && !this.sourceRegistry.has(task.source.pluginType)) {
            throw new Error(`Update failed for task "${task.name}": Source plugin "${task.source.pluginType}" not registered.`);
        }
        // Safely access task.destination properties after checking it exists
        if (task.destination && updates.destination && !this.destinationRegistry.has(task.destination.pluginType)) {
            throw new Error(`Update failed for task "${task.name}": Destination plugin "${task.destination.pluginType}" not registered.`);
        }

        task.currentStatus = 'scheduled'; // Reset status after update

        // Re-setup trigger if enabled status or trigger type changed
        if (originalEnabled !== task.enabled || (updates.trigger && updates.trigger.type !== originalTriggerType)) {
            this._setupTrigger(task);
        }

        logger.info(`Task "${task.name}" (ID: ${task.id}) updated.`); // task.id is guaranteed string here
        return task;
    }

    /**
     * Deletes an ingestion task from the manager.
     * @param taskId The ID of the task to delete.
     * @returns True if deleted, false if not found.
     */
    async deleteTask(taskId: string): Promise<boolean> {
        if (await this.descheduleTask(taskId)) { // Deschedule first to clean up resources
            const deleted = this.tasks.delete(taskId);
            if (deleted) {
                logger.info(`Task ${taskId} deleted.`);
            }
            return deleted;
        }
        logger.warn(`Task ${taskId} could not be descheduled/deleted.`);
        return false;
    }

    /**
     * Gets the current status of a specific ingestion task.
     * @param taskId The ID of the task.
     * @returns The IngestionTask object or undefined if not found.
     */
    getTaskStatus(taskId: string): IngestionTask | undefined {
        return this.tasks.get(taskId);
    }

    /**
     * Lists all currently managed ingestion tasks.
     * @returns An array of all IngestionTask objects.
     */
    listTasks(): IngestionTask[] {
        return Array.from(this.tasks.values());
    }

    /**
     * Manually triggers an ingestion task.
     * @param taskId The ID of the manual task.
     * @returns A GSStatus indicating the result of the manual trigger.
     * @throws If the task is not found or not configured for manual trigger.
     */
    async triggerManualTask(taskId: string): Promise<GSStatus> {
        const task = this.tasks.get(taskId);
        if (!task) {
            return new GSStatus(false, 404, `Task with ID "${taskId}" not found.`);
        }
        if (task.trigger.type !== 'manual') {
            return new GSStatus(false, 400, `Task "${taskId}" is not configured for manual triggering.`);
        }
        logger.info(`Manually triggering task: ${taskId}`);
        return await this._executeIngestionTask(taskId, task);
    }

    /**
     * Triggers an ingestion task via a simulated webhook call.
     * @param endpointId The endpoint ID configured for the webhook trigger.
     * @param payload Optional payload for the webhook.
     * @returns A GSStatus indicating the result of the webhook trigger.
     */
    async triggerWebhookTask(endpointId: string, payload?: any): Promise<GSStatus> {
        // In a real system, this would map endpointId to tasks.
        // For simplicity, we assume endpointId is the taskId for now.
        const task = this.listTasks().find(t => t.trigger.type === 'webhook' && t.trigger.endpointId === endpointId);

        if (!task) {
            return new GSStatus(false, 404, `Webhook endpoint "${endpointId}" not associated with any task.`);
        }

        logger.info(`Webhook triggering task: ${task.id} with payload: ${JSON.stringify(payload)}`); // task.id is guaranteed string here
        return await this._executeIngestionTask(task.id, task);
    }


    /**
     * Starts the GlobalIngestionLifecycleManager, activating all scheduled tasks.
     */
    start(): void {
        if (this.lifecycleStarted) {
            logger.warn("GlobalIngestionLifecycleManager is already started.");
            return;
        }
        logger.info("Starting GlobalIngestionLifecycleManager...");
        this.tasks.forEach(task => {
            this._setupTrigger(task);
        });
        this.lifecycleStarted = true;
        logger.info("GlobalIngestionLifecycleManager started. Triggers are active.");
    }

    /**
     * Stops the GlobalIngestionLifecycleManager, clearing all active cron intervals.
     */
    stop(): void {
        if (!this.lifecycleStarted) {
            logger.warn("GlobalIngestionLifecycleManager is already stopped.");
            return;
        }
        logger.info("Stopping GlobalIngestionLifecycleManager...");
        this.tasks.forEach(task => {
            if (task.trigger.type === 'cron' && task.trigger._intervalId) {
                clearInterval(task.trigger._intervalId);
                delete task.trigger._intervalId;
                logger.info(`Cleared cron for task ${task.id}.`); // task.id is guaranteed string here
            }
        });
        this.lifecycleStarted = false;
        logger.info("GlobalIngestionLifecycleManager stopped. All cron timers cleared.");
    }

    /**
     * Internal method to set up the appropriate trigger for a task (cron, webhook).
     * @param task The ingestion task to set up the trigger for.
     */
    private _setupTrigger(task: IngestionTask): void {
        if (!task.enabled) {
            logger.debug(`Task ${task.id} is disabled, not setting up trigger.`);
            if (task.trigger.type === 'cron' && task.trigger._intervalId) {
                clearInterval(task.trigger._intervalId); // Clear existing if task becomes disabled
                delete task.trigger._intervalId;
            }
            return;
        }

        switch (task.trigger.type) {
            case 'cron':
                const cronTrigger = task.trigger;
                // Clear any existing interval to prevent duplicates if trigger changes
                if (cronTrigger._intervalId) {
                    clearInterval(cronTrigger._intervalId);
                }
                logger.info(`Scheduling cron-like trigger for task ${task.id} with expression "${cronTrigger.expression}"`); // task.id is guaranteed string here
                const intervalMs = 5000; // Trigger every 5 seconds for demo purposes
                cronTrigger._intervalId = setInterval(async () => {
                    logger.info(`Cron trigger activated for task: ${task.id}`); // task.id is guaranteed string here
                    const status = await this._executeIngestionTask(task.id, task);
                    logger.info(`Cron task ${task.id} finished with status: ${status.success ? 'SUCCESS' : 'FAILURE'}`); // task.id is guaranteed string here
                }, intervalMs);
                task.nextRun = new Date(Date.now() + intervalMs);
                break;
            case 'webhook':
                // The 'endpointId' in WebhookTrigger is already defined as 'string'
                logger.info(`Webhook trigger configured for task ${task.id} at endpointId: ${task.trigger.endpointId}. Waiting for external trigger.`); // task.id is guaranteed string here
                break;
            case 'manual':
                logger.info(`Task ${task.id} is configured for manual triggering only.`); // task.id is guaranteed string here
                break;
            default:
                logger.warn(`Unknown trigger type for task ${task.id}. No trigger set up.`); // task.id is guaranteed string here
                break;
        }
    }

    /**
     * Internal method to execute an actual ingestion task using the Orchestrator.
     * This method also hooks into the orchestrator's events and re-emits them.
     * @param taskId The ID of the task being executed.
     * @param taskDef The full definition of the task.
     * @returns A GSStatus indicating the result of the ingestion execution.
     */
    private async _executeIngestionTask(taskId: string, taskDef: IngestionTask): Promise<GSStatus> {
        // taskDef.id is now guaranteed to be a string due to the assignment in scheduleTask
        taskDef.currentStatus = 'running';
        taskDef.lastRun = new Date();
        logger.info(`Executing ingestion task: "${taskDef.name}" (ID: ${taskDef.id})`);

        let executionStatus: GSStatus;

        try {
            const sourceRegistration = this.sourceRegistry.get(taskDef.source.pluginType);
            if (!sourceRegistration) {
                throw new Error(`Source plugin "${taskDef.source.pluginType}" not found in registry.`);
            }

            const SourceConstructor = sourceRegistration.constructor;
            const transformer = sourceRegistration.transformer;

            const dataSourceInstance = new SourceConstructor({ config: taskDef.source.config });

            if (typeof dataSourceInstance.initClient === 'function') {
                logger.info(`Initializing client for ${taskDef.source.pluginType} for task ${taskDef.id}...`);
                await dataSourceInstance.initClient();
                logger.info(`Client initialized for ${taskDef.source.pluginType}.`);
            }

            let destinationInstance: IDestinationPlugin | undefined;
            if (taskDef.destination) {
                const DestinationConstructor = this.destinationRegistry.get(taskDef.destination.pluginType);
                if (!DestinationConstructor) {
                    throw new Error(`Destination plugin "${taskDef.destination.pluginType}" not found in registry.`);
                }
                destinationInstance = new DestinationConstructor();
                // Initialize destination plugin with its specific config
                // TypeScript now knows taskDef.destination is not undefined inside this block
                await destinationInstance.init(taskDef.destination.config);
                logger.info(`Destination plugin "${taskDef.destination.pluginType}" initialized for task ${taskDef.id}.`);
            }

            const orchestrator = new IngestionOrchestrator();
            orchestrator.configure(dataSourceInstance, transformer, destinationInstance);

            // Hook into the orchestrator's events and re-emit them with taskId context
            orchestrator.getEventBus().on(IngestionEvents.DATA_RECEIVED, (data: IngestionData[]) => {
                this.eventBus.emit(IngestionEvents.DATA_RECEIVED, data, taskDef.id as string); // Cast to string
            });
            orchestrator.getEventBus().on(IngestionEvents.TASK_COMPLETED, (status: GSStatus) => {
                this.eventBus.emit(IngestionEvents.TASK_COMPLETED, status, taskDef.id as string); // Cast to string
            });
            orchestrator.getEventBus().on(IngestionEvents.TASK_FAILED, (status: GSStatus) => {
                this.eventBus.emit(IngestionEvents.TASK_FAILED, status, taskDef.id as string); // Cast to string
            });

            executionStatus = await orchestrator.executeTask();
            taskDef.currentStatus = executionStatus.success ? 'completed' : 'failed';

        } catch (error: any) {
            logger.error(`Error executing task ${taskDef.id} - "${taskDef.name}": ${error.message}`, { error: error });
            executionStatus = new GSStatus(false, 500, `Task execution failed: ${error.message}`, { error: error.message });
            taskDef.currentStatus = 'failed';
        }

        taskDef.lastRunStatus = executionStatus;
        logger.info(`Task ${taskDef.id} - "${taskDef.name}" execution finished with status: ${taskDef.currentStatus}.`);
        return executionStatus;
    }
}