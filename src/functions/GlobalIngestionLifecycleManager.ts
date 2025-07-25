// C:\Users\SOHAM\Desktop\crawler\test-crawler\src\functions\GlobalIngestionLifecycleManager.ts

import { EventEmitter } from 'events';
import { CronExpressionParser } from 'cron-parser';
// Import essential core types and objects directly from @godspeedsystems/core
// FIX: Import the entire module as 'Godspeed' to access nested types/enums

import { GSStatus, logger, GSContext, GSCloudEvent, GSActor } from '@godspeedsystems/core'; // Keep direct imports for classes/logger if they are truly direct exports

// Corrected import for cron-parser's parseExpression - using default import as suggested by error
import parseExpression from 'cron-parser';

import { randomUUID } from 'crypto';

import {
    IGlobalIngestionLifecycleManager,
    IngestionTaskDefinition,
    GSDataSource,
    IngestionDataTransformer,
    IDestinationPlugin,
    CronTrigger,
    WebhookTrigger,
    IngestionData,
    IngestionTrigger,
    IngestionEvents,
    IngestionTaskStatus
} from './ingestion/interfaces';

import { IngestionOrchestrator } from './ingestion/orchestrator';

// Define a type for a Godspeed DataSource constructor with initClient
type DataSourceConstructor = new (options: { config: any }) => GSDataSource & { initClient?: () => Promise<object> };
// Define a type for a Destination Plugin constructor
type DestinationConstructor = new () => IDestinationPlugin;

export class GlobalIngestionLifecycleManager implements IGlobalIngestionLifecycleManager {
    private tasks: Map<string, IngestionTaskDefinition> = new Map();
    private sourcePlugins: Map<string, { plugin: DataSourceConstructor, transformer: IngestionDataTransformer }> = new Map();
    private destinationPlugins: Map<string, DestinationConstructor> = new Map();
    private orchestrators: Map<string, IngestionOrchestrator> = new Map();
    private eventBus: EventEmitter = new EventEmitter();
    private lifecycleStarted: boolean = false;

    constructor() {
        logger.info('GlobalIngestionLifecycleManager initialized.');
    }

    public async init(): Promise<void> {
        logger.info('GlobalIngestionLifecycleManager init called.');
        return Promise.resolve();
    }

    public async start(): Promise<void> {
        if (this.lifecycleStarted) {
            logger.warn('GlobalIngestionLifecycleManager is already started.');
            return Promise.resolve();
        }
        this.lifecycleStarted = true;
        logger.info('GlobalIngestionLifecycleManager started. Setting up triggers for enabled tasks.');
        this.tasks.forEach(task => this._setupTrigger(task));
        this.eventBus.emit(IngestionEvents.TASK_TRIGGERED, 'manager:start', 'Manager started');
        return Promise.resolve();
    }

    public async stop(): Promise<void> {
        if (!this.lifecycleStarted) {
            logger.warn('GlobalIngestionLifecycleManager is not running.');
            return Promise.resolve();
        }
        logger.info('GlobalIngestionLifecycleManager stopping. Clearing all active triggers.');
        this.tasks.forEach(task => this._clearTrigger(task));
        this.lifecycleStarted = false;
        this.eventBus.emit(IngestionEvents.TASK_TRIGGERED, 'manager:stop', 'Manager stopped');
        return Promise.resolve();
    }

    public registerSource(pluginType: string, sourcePlugin: DataSourceConstructor, transformer: IngestionDataTransformer): void {
        if (this.sourcePlugins.has(pluginType)) {
            logger.warn(`Source plugin '${pluginType}' already registered. Overwriting.`);
        }
        this.sourcePlugins.set(pluginType, { plugin: sourcePlugin, transformer });
        logger.info(`Source plugin '${pluginType}' registered.`);
    }

    public registerDestination(pluginType: string, destinationPlugin: DestinationConstructor): void {
        if (this.destinationPlugins.has(pluginType)) {
            logger.warn(`Destination plugin '${pluginType}' already registered. Overwriting.`);
        }
        this.destinationPlugins.set(pluginType, destinationPlugin);
        logger.info(`Destination plugin '${pluginType}' registered.`);
    }

    public async scheduleTask(taskDefinition: IngestionTaskDefinition): Promise<GSStatus> {
        const taskId: string = taskDefinition.id || randomUUID();
        if (this.tasks.has(taskId)) {
            logger.warn(`Task '${taskId}' already exists. Use updateTask to modify.`);
            return { success: false, message: `Task '${taskId}' already exists.` };
        }

        taskDefinition.id = taskId;

        taskDefinition.currentStatus = IngestionTaskStatus.SCHEDULED;
        taskDefinition.lastRun = undefined;
        taskDefinition.lastRunStatus = undefined;

        this.tasks.set(taskId, taskDefinition);
        logger.info(`Task '${taskId}' scheduled.`);
        this.eventBus.emit(IngestionEvents.TASK_SCHEDULED, taskId, taskDefinition);

        if (this.lifecycleStarted) {
            this._setupTrigger(taskDefinition);
        }
        return { success: true, message: `Task '${taskId}' scheduled successfully.` };
    }

    public async updateTask(taskId: string, updates: Partial<IngestionTaskDefinition>): Promise<GSStatus> {
        const task = this.tasks.get(taskId);
        if (!task) {
            logger.warn(`Task '${taskId}' not found for update.`);
            return { success: false, message: `Task '${taskId}' not found.` };
        }

        this._clearTrigger(task);

        const updatedTask = { ...task, ...updates };
        this.tasks.set(taskId, updatedTask);
        logger.info(`Task '${taskId}' updated.`);
        this.eventBus.emit(IngestionEvents.TASK_UPDATED, taskId, updatedTask);

        if (this.lifecycleStarted) {
            this._setupTrigger(updatedTask);
        }
        return { success: true, message: `Task '${taskId}' updated successfully.` };
    }

    public async enableTask(taskId: string): Promise<GSStatus> {
        const task = this.tasks.get(taskId);
        if (!task) {
            return { success: false, message: `Task '${taskId}' not found.` };
        }
        if (task.enabled) {
            logger.info(`Task '${taskId}' is already enabled.`);
            return { success: true, message: `Task '${taskId}' is already enabled.` };
        }
        task.enabled = true;
        task.currentStatus = IngestionTaskStatus.SCHEDULED;
        logger.info(`Task '${taskId}' enabled.`);
        this.eventBus.emit(IngestionEvents.TASK_ENABLED, taskId, task);
        if (this.lifecycleStarted) {
            this._setupTrigger(task);
        }
        return { success: true, message: `Task '${taskId}' enabled successfully.` };
    }

    public async disableTask(taskId: string): Promise<GSStatus> {
        const task = this.tasks.get(taskId);
        if (!task) {
            return { success: false, message: `Task '${taskId}' not found.` };
        }
        if (!task.enabled) {
            logger.info(`Task '${taskId}' is already disabled.`);
            return { success: true, message: `Task '${taskId}' is already disabled.` };
        }
        task.enabled = false;
        task.currentStatus = IngestionTaskStatus.DISABLED;
        logger.info(`Task '${taskId}' disabled.`);
        this.eventBus.emit(IngestionEvents.TASK_DISABLED, taskId, task);
        this._clearTrigger(task);
        return { success: true, message: `Task '${taskId}' disabled successfully.` };
    }

    public async deleteTask(taskId: string): Promise<GSStatus> {
        const task = this.tasks.get(taskId);
        if (!task) {
            return { success: false, message: `Task '${taskId}' not found.` };
        }
        this._clearTrigger(task);
        this.tasks.delete(taskId);
        this.orchestrators.delete(taskId);
        logger.info(`Task '${taskId}' deleted.`);
        this.eventBus.emit(IngestionEvents.TASK_DELETED, taskId);
        return { success: true, message: `Task '${taskId}' deleted successfully.` };
    }

    public getTask(taskId: string): IngestionTaskDefinition | undefined {
        return this.tasks.get(taskId);
    }

    public listTasks(): IngestionTaskDefinition[] {
        return Array.from(this.tasks.values());
    }

    public async triggerManualTask(ctx: GSContext,taskId: string, initialPayload?: any): Promise<GSStatus> {
        const task = this.tasks.get(taskId);
        if (!task) {
            logger.warn(`Manual trigger failed: Task '${taskId}' not found.`);
            return { success: false, message: `Task '${taskId}' not found.` };
        }
        if (!task.enabled) {
            logger.warn(`Manual trigger failed: Task '${taskId}' is disabled.`);
            return { success: false, message: `Task '${taskId}' is disabled.` };
        }
        logger.info(`Manual trigger activated for task '${taskId}'.`);
        // Pass a minimal context here
        return this._executeIngestionTask(ctx,task.id, task, initialPayload);
    }

    public async triggerWebhookTask(ctx: GSContext,endpointId: string, payload: any): Promise<GSStatus> {
        logger.info(`Webhook trigger received for endpointId: '${endpointId}'.`);
        const tasksToTrigger = Array.from(this.tasks.values()).filter(
            task => task.enabled && task.trigger.type === 'webhook' && (task.trigger as WebhookTrigger).endpointId === endpointId
        );

        if (tasksToTrigger.length === 0) {
            logger.warn(`No enabled webhook task found for endpointId: '${endpointId}'.`);
            return { success: false, message: `No enabled webhook task found for endpointId: '${endpointId}'.` };
        }

        const results: { taskId: string; status: GSStatus }[] = [];
        for (const task of tasksToTrigger) {
            logger.info(`Executing webhook-triggered task: ${task.id} with payload.`);
            // Pass a minimal context here
            const status = await this._executeIngestionTask(ctx,task.id, task, { webhookPayload: payload });
            results.push({ taskId: task.id, status });
        }

        const successful = results.filter(r => r.status.success).length;
        const failed = results.length - successful;
        if (failed > 0) {
            return { success: false, message: `Webhook triggered ${results.length} tasks. ${successful} succeeded, ${failed} failed.`, data: results };
        }
        return { success: true, message: `Webhook successfully triggered ${successful} tasks.`, data: results };
    }

    public async triggerAllEnabledCronTasks(ctx: GSContext): Promise<GSStatus> {
    logger.info("Manager received command to trigger all enabled cron tasks. Checking due tasks...");
    // FIX: Use ctx.event?.time for 'now' to align with Godspeed's event timestamp, with fallback
    const now = new Date((ctx as any).event?.time || new Date().toISOString());
    const results: { taskId: string; status: GSStatus }[] = [];
    let tasksDueCount = 0;

    for (const task of this.tasks.values()) {
        logger.info("TASK (from glb)", { task });

        if (task.enabled && task.trigger.type === 'cron') {
            const cronTrigger = task.trigger as CronTrigger;
            try {
                // Use 'now' (derived from ctx.event.time) for currentDate in cron-parser
                const interval = CronExpressionParser.parse(cronTrigger.expression, { currentDate: now });
                const previousRunTime = interval.prev().toDate(); // Last scheduled time before or at 'now'

                // FIX: Define a wider, robust window (e.g., 65 seconds)
                const sixtyFiveSecondsAgo = new Date(now.getTime() - (65 * 1000));

                // FIX: Modified checking condition with the robust window and lastRun check
                // Condition:
                // 1. previousRunTime must be after the 'sixtyFiveSecondsAgo' mark (it's recent)
                // 2. previousRunTime must be at or before 'now' (it's not in the future)
                // 3. AND (crucially) the task's lastRun must be undefined (never run)
                //    OR the task's lastRun must be older than this specific previousRunTime.
                //    This prevents re-running a task for the same scheduled interval if the trigger fires multiple times.
                if (previousRunTime > sixtyFiveSecondsAgo && previousRunTime <= now &&
                    (!task.lastRun || task.lastRun < previousRunTime)) {

                    logger.info(`Executing cron-triggered task: ${task.id} (expression: ${cronTrigger.expression}, last due: ${previousRunTime.toISOString()}).`);
                    tasksDueCount++;
                    // Pass a minimal context here
                    const status = await this._executeIngestionTask(ctx, task.id, task);
                    results.push({ taskId: task.id, status });
                } else {
                    // FIX: Updated debug log to use sixtyFiveSecondsAgo
                    logger.debug(`Task '${task.id}' (cron: ${cronTrigger.expression}) not due. ` +
                                      `prevRun: ${previousRunTime.toISOString()}, ` +
                                      `now: ${now.toISOString()}, ` +
                                      `sixtyFiveSecondsAgo: ${sixtyFiveSecondsAgo.toISOString()}. ` +
                                      `lastRun: ${task.lastRun ? task.lastRun.toISOString() : 'never'}.`);
                }
            } catch (error: any) {
                logger.error(`Error parsing cron expression for task '${task.id}': ${cronTrigger.expression}. Error: ${error.message}`);
                results.push({ taskId: task.id, status: { success: false, message: `Cron expression parse error: ${error.message}` } });
            }
        }
    }

    if (tasksDueCount === 0) {
        logger.info("No enabled cron tasks were due at this time.");
        return { success: true, message: "No enabled cron tasks were due." };
    }

    const successful = results.filter(r => r.status.success).length;
    const failed = results.length - successful;
    if (failed > 0) {
        return { success: false, message: `Cron triggered ${tasksDueCount} tasks. ${successful} succeeded, ${failed} failed.`, data: results };
    }
    return { success: true, message: `Successfully triggered ${successful} cron tasks.` };
}

    public getEventBus(): EventEmitter {
        return this.eventBus;
    }

    private async _executeIngestionTask(ctx: GSContext,taskId: string, task: IngestionTaskDefinition, initialPayload?: any): Promise<GSStatus> {
        if (!task.enabled) {
            logger.warn(`Attempted to execute disabled task '${taskId}'. Skipping.`);
            return { success: false, message: `Task '${taskId}' is disabled.`, data: { code: 'TASK_DISABLED' } };
        }

        const sourceDef = this.sourcePlugins.get(task.source.pluginType);

        let destinationDef: (new (...args: any[]) => IDestinationPlugin) | undefined;
        if (task.destination) {
            destinationDef = this.destinationPlugins.get(task.destination.pluginType);
        }

        if (!sourceDef) {
            logger.error(`Source plugin '${task.source.pluginType}' not registered for task '${taskId}'.`);
            const status: GSStatus = { success: false, message: `Source plugin '${task.source.pluginType}' not registered.` };
            task.currentStatus = IngestionTaskStatus.FAILED;
            task.lastRunStatus = status;
            this.eventBus.emit(IngestionEvents.TASK_FAILED, taskId, status);
            return status;
        }
        if (task.destination && !destinationDef) {
            logger.error(`Destination plugin '${task.destination.pluginType}' not registered for task '${taskId}'.`);
            const status: GSStatus = { success: false, message: `Destination plugin '${task.destination.pluginType}' not registered.` };
            task.currentStatus = IngestionTaskStatus.FAILED;
            task.lastRunStatus = status;
            this.eventBus.emit(IngestionEvents.TASK_FAILED, taskId, status);
            return status;
        }

        let orchestrator = this.orchestrators.get(taskId);
        if (!orchestrator) {
            const sourcePluginInstance = new sourceDef.plugin({ config: task.source.config });

            let destinationPluginInstance: IDestinationPlugin | undefined;
            if (task.destination && destinationDef) {
                destinationPluginInstance = new destinationDef();
                await destinationPluginInstance.init(task.destination.config);
            }

            orchestrator = new IngestionOrchestrator(
                sourcePluginInstance,
                sourceDef.transformer,
                destinationPluginInstance,
                this.eventBus,
                taskId
            );
            this.orchestrators.set(taskId, orchestrator);
        }

        task.currentStatus = IngestionTaskStatus.RUNNING;
        task.lastRun = new Date();
        logger.info(`Executing task '${task.name}' (ID: ${task.id}).`);
        this.eventBus.emit(IngestionEvents.TASK_TRIGGERED, taskId);

        let executionStatus: GSStatus;

        try {
            

            executionStatus = await orchestrator.executeTask(ctx, initialPayload); // Pass minimalContext first
            task.lastRunStatus = executionStatus;
            if (executionStatus.success) {
                logger.info(`Task '${taskId}' completed successfully.`);
                task.currentStatus = IngestionTaskStatus.COMPLETED;
                this.eventBus.emit(IngestionEvents.TASK_COMPLETED, taskId, executionStatus);
            } else {
                logger.error(`Task '${taskId}' failed: ${executionStatus.message}`);
                task.currentStatus = IngestionTaskStatus.FAILED;
                this.eventBus.emit(IngestionEvents.TASK_FAILED, taskId, executionStatus);
            }
        } catch (error: any) {
            logger.error(`Unhandled error during execution of task '${taskId}': ${error.message}`, { error: error });
            executionStatus = { success: false, message: `Unhandled error during task execution: ${error.message}` };
            task.currentStatus = IngestionTaskStatus.FAILED;
            task.lastRunStatus = executionStatus;
            this.eventBus.emit(IngestionEvents.TASK_FAILED, taskId, executionStatus);
        } finally {
            if (task.currentStatus !== IngestionTaskStatus.FAILED && task.currentStatus !== IngestionTaskStatus.COMPLETED) {
                task.currentStatus = task.enabled ? IngestionTaskStatus.SCHEDULED : IngestionTaskStatus.DISABLED;
            }
        }
        logger.info(`Task ${task.id} - "${task.name}" execution finished with status: ${task.currentStatus}.`);
        return executionStatus;
    }

    private _setupTrigger(task: IngestionTaskDefinition): void {
        if (!task.enabled || !this.lifecycleStarted) {
            this._clearTrigger(task);
            return;
        }

        switch (task.trigger.type) {
            case 'cron':
                const cronTrigger = task.trigger as CronTrigger;
                // Combined into a single template literal for robustness
                logger.info(`Task '${task.id}' is configured for Godspeed Cron trigger "${cronTrigger.expression}". Ensure a Godspeed cron event is set up to call triggerAllEnabledCronTasks().`);
                break;

            case 'webhook':
                const webhookTrigger = task.trigger as WebhookTrigger;
                logger.info(`Task '${task.id}' configured for webhook trigger at endpointId: '${webhookTrigger.endpointId}'.`);
                break;

            case 'manual':
                logger.info(`Task '${task.id}' is configured for manual trigger.`);
                break;

            default:
                logger.warn(`Unknown trigger type for task '${task.id}'. No trigger set up.`);
                break;
        }
    }

    private _clearTrigger(task: IngestionTaskDefinition): void {
        switch (task.trigger.type) {
            case 'cron':
                logger.debug(`No internal cron timer to clear for task '${task.id}' (managed externally).`);
                break;
            case 'webhook':
                logger.debug(`Webhook for task '${task.id}' has no internal timer to clear.`);
                break;
            case 'manual':
                logger.debug(`Manual task '${task.id}' has no internal timer to clear.`);
                break;
            default:
                break;
        }
    }
}