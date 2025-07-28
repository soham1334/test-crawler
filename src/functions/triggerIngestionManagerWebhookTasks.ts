// src/functions/triggerIngestionManagerWebhookTasks.ts
import { GSContext, GSStatus, logger } from "@godspeedsystems/core";
import { globalIngestionManager } from "./test-run";
import * as crypto from 'crypto';

export default  async function (ctx: GSContext): Promise<GSStatus> {
    logger.info("Received webhook event.");
    
    const webhookPayload = (ctx.inputs as any).data.body; 
    const requestHeaders = (ctx.inputs as any).data.headers;
    const eventTypePath = (ctx.inputs as any).type; // Get the CloudEvent type, e.g., '/webhook/github/'

    let taskId: string | undefined;

    // FIX: Determine taskId based on the eventTypePath
    if (eventTypePath === '/webhook/github/') {
        taskId = 'webhook-git-crawl'; // Matches the 'id' in test-run.ts and events.yaml
    } else if (eventTypePath === '/webhook/http-data/') {
        taskId = 'http-data-event'; // Matches the 'id' in test-run.ts and events.yaml
    } else {
        logger.error(`Unknown eventTypePath received: '${eventTypePath}'. Cannot determine task ID.`);
        return { success: false, code: 400, message: `Unknown webhook event type: ${eventTypePath}` };
    }
    console.log("--------------START TASK LIST------------")
    console.log(globalIngestionManager.listTasks())
    console.log("--------------END TASK LIST------------")
    logger.debug(`[Webhook Handler Debug] Received webhook for eventTypePath: '${eventTypePath}'. Derived taskId: '${taskId}'.`);
    logger.debug(`[Webhook Handler Debug] Extracted webhookPayload: ${JSON.stringify(webhookPayload)?.substring(0, 200)}...`);
    logger.debug(`[Webhook Handler Debug] Extracted requestHeaders: ${JSON.stringify(requestHeaders)}`);

    if (!webhookPayload) {
        logger.warn("Webhook received with no payload.");
        return { success: false, message: "No webhook payload received." };
    }

    // // --- Webhook Secret Validation (only for GitHub webhook) ---
    // if (taskId === 'github-push-event') { // Only validate for GitHub push events
    //     const task = globalIngestionManager.getTask(taskId); 
    //     const webhookSecret = (task?.source?.config as any)?.webhookSecret; 

    //     if (webhookSecret) {
    //         const signature = requestHeaders['x-hub-signature-256'] || requestHeaders['x-hub-signature']; 
    //         if (!signature) {
    //             logger.error("GitHub Webhook received without signature. Rejecting.");
    //             return { success: false, code: 401, message: "Missing X-Hub-Signature header." };
    //         }

    //         const hmac = crypto.createHmac('sha256', webhookSecret); 
    //         hmac.update(JSON.stringify(webhookPayload)); 
    //         const digest = `sha256=${hmac.digest('hex')}`;

    //         if (digest !== signature) {
    //             logger.error("GitHub Webhook signature mismatch. Rejecting.");
    //             return { success: false, code: 403, message: "Invalid webhook signature." };
    //         }
    //         logger.info("GitHub Webhook signature validated successfully.");
    //     } else {
    //         logger.warn(`No webhookSecret configured for task '${taskId}'. Skipping signature validation.`);
    //     }
    // }
    // // --- END Webhook Secret Validation ---


    logger.info(`Triggering webhook task for task ID: '${taskId}' with payload:`, JSON.stringify(webhookPayload)?.substring(0, 200) + '...');

    const result = await globalIngestionManager.triggerManualTask(ctx, taskId, { webhookPayload }); 

    if (result.success) {
        logger.info(`Webhook task '${taskId}' successfully triggered: ${result.message}`);
    } else {
        logger.error(`Failed to trigger webhook task '${taskId}': ${result.message}`, result.data);
    }

    return result;
}
