// src/functions/triggerIngestionManagerCronTasks.ts
import { GSContext, GSStatus } from "@godspeedsystems/core";
import { globalIngestionManager } from "./test-run";
import { IngestionTaskStatus, IngestionTaskDefinition } from "./ingestion/interfaces"; // Import IngestionTaskStatus and IngestionTaskDefinition

/**
 * Godspeed function triggered by a cron event.
 *
 * This function acts as a bridge to trigger all enabled cron tasks
 * and also demonstrates the functionality of various GlobalIngestionLifecycleManager methods.
 *
 * @param ctx The Godspeed context object, provided by the framework.
 * @returns A GSStatus indicating the outcome of triggering the ingestion tasks.
 */
export default async function (ctx: GSContext): Promise<GSStatus> {
    const { logger } = ctx;

    logger.info("--- Received cron trigger. Initiating comprehensive manager method check. ---");
    
    // --- 1. Trigger All Enabled Cron Tasks (Primary Purpose) ---
    logger.info("Attempting to trigger all enabled cron tasks via GlobalIngestionLifecycleManager.");
    const cronTriggerResult = await globalIngestionManager.triggerAllEnabledCronTasks(ctx);
    if (cronTriggerResult.success) {
        logger.info(`GlobalIngestionLifecycleManager successfully processed cron tasks: ${cronTriggerResult.message}`, cronTriggerResult.data);
    } else {
        logger.error(`GlobalIngestionLifecycleManager encountered errors during cron task processing: ${cronTriggerResult.message}`, cronTriggerResult.data);
    }

    // --- 2. Demonstrate Other Manager Methods ---

    // a. List all tasks initially
    let tasks = globalIngestionManager.listTasks();
    logger.info(`[Manager Check] Total tasks currently registered: ${tasks.length}`, { taskIds: tasks.map(t => t.id) });

    // // b. Schedule a new manual task
    // const newManualTaskId = 'test-manual-task-' + Date.now(); // Unique ID for each run
    // const newManualTask: IngestionTaskDefinition = {
    //     id: newManualTaskId,
    //     name: 'Dynamically Scheduled Manual Task',
    //     enabled: true,
    //     trigger: { type: 'manual' },
    //     source: { pluginType: 'http-crawler', config: { startUrl: 'https://www.google.com/' } }, // Use a simple URL
    //     destination: { pluginType: 'file-system-destination', config: { outputPath: './crawled_output/dynamic-tasks' } },
    //     currentStatus: IngestionTaskStatus.SCHEDULED,
    // };
    // const scheduleResult = await globalIngestionManager.scheduleTask(newManualTask);
    // logger.info(`[Manager Check] Schedule new task '${newManualTaskId}': ${scheduleResult.message}`, { success: scheduleResult.success });
    // tasks = globalIngestionManager.listTasks();
    // logger.info(`[Manager Check] Tasks after scheduling: ${tasks.length}`, { taskIds: tasks.map(t => t.id) });

    // // c. Get the newly scheduled task
    // const fetchedTask = globalIngestionManager.getTask(newManualTaskId);
    // logger.info(`[Manager Check] Fetched task '${newManualTaskId}':`, fetchedTask ? { id: fetchedTask.id, name: fetchedTask.name, status: fetchedTask.currentStatus } : 'Not found');

    // // d. Update an existing task (e.g., the 'cron-http-crawl-daily' task)
    // const existingTaskIdToUpdate = 'cron-http-crawl-daily'; // ID from test-run.ts
    // const updateResult = await globalIngestionManager.updateTask(existingTaskIdToUpdate, {
    //     description: 'Updated description from cron trigger function test',
    //     enabled: true // Ensure it stays enabled
    // });
    // logger.info(`[Manager Check] Update task '${existingTaskIdToUpdate}': ${updateResult.message}`, { success: updateResult.success });
    // const updatedTask = globalIngestionManager.getTask(existingTaskIdToUpdate);
    // logger.info(`[Manager Check] Updated task '${existingTaskIdToUpdate}' status: ${updatedTask?.currentStatus}, desc: ${updatedTask?.description}`);

    // // e. Disable the new manual task
    // const disableResult = await globalIngestionManager.disableTask(newManualTaskId);
    // logger.info(`[Manager Check] Disable task '${newManualTaskId}': ${disableResult.message}`, { success: disableResult.success });
    // const disabledTask = globalIngestionManager.getTask(newManualTaskId);
    // logger.info(`[Manager Check] Task '${newManualTaskId}' status after disable: ${disabledTask?.currentStatus}`);

    // // f. Enable the new manual task
    // const enableResult = await globalIngestionManager.enableTask(newManualTaskId);
    // logger.info(`[Manager Check] Enable task '${newManualTaskId}': ${enableResult.message}`, { success: enableResult.success });
    // const enabledTask = globalIngestionManager.getTask(newManualTaskId);
    // logger.info(`[Manager Check] Task '${newManualTaskId}' status after enable: ${enabledTask?.currentStatus}`);

    // // g. Trigger the newly enabled manual task
    // const manualTriggerResult = await globalIngestionManager.triggerManualTask(ctx, newManualTaskId, { triggeredBy: 'cron_function_test' });
    // logger.info(`[Manager Check] Manual trigger for '${newManualTaskId}': ${manualTriggerResult.message}`, { success: manualTriggerResult.success });
    // const manualTriggeredTask = globalIngestionManager.getTask(newManualTaskId);
    // logger.info(`[Manager Check] Task '${newManualTaskId}' status after manual trigger: ${manualTriggeredTask?.currentStatus}`);

    // // h. Delete the new manual task
    // const deleteResult = await globalIngestionManager.deleteTask(newManualTaskId);
    // logger.info(`[Manager Check] Delete task '${newManualTaskId}': ${deleteResult.message}`, { success: deleteResult.success });
    // const deletedTask = globalIngestionManager.getTask(newManualTaskId);
    // logger.info(`[Manager Check] Task '${newManualTaskId}' exists after delete? ${deletedTask ? 'Yes' : 'No'}`);
    // tasks = globalIngestionManager.listTasks();
    // logger.info(`[Manager Check] Total tasks after deletion: ${tasks.length}`, { taskIds: tasks.map(t => t.id) });

    // logger.info("--- Comprehensive manager method check complete. ---");

    return cronTriggerResult; // Return the status of the primary cron trigger operation
}
