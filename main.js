import fs from 'fs'; // Core Node.js module for file system operations
import path from 'path'; // Core Node.js module for handling file paths
import puppeteer from 'puppeteer'; // Library for browser automation

// GLOBAL CONFIGURATION

// Browser Configuration
const BROWSER_HEADLESS_MODE = false; // Set to false to run with a visible GUI
const NAVIGATION_TIMEOUT_MS = 60000; // 60 seconds for all navigation/API calls

// File System Configuration
const BASE_ASSET_DIR = 'assets';
const OUTPUT_FILE_EXTENSION = '.txt';
const FILE_VERSION_SUFFIX = '-1'; // Suffix used in the expected filename (e.g., 'sandpoint-ak-1.txt')

// API Domain and Endpoints
const API_DOMAIN = 'https://codelibrary.amlegal.com';
const DOWNLOAD_DOMAIN = 'https://export.amlegal.com';

const REGIONS_API_PATH = '/api/client-regions/';
const EXPORT_API_PATH = '/api/export-requests/';
const CLIENT_API_PREFIX = '/api/clients/';
const VERSION_API_PREFIX = '/api/code-versions/';

// Request Parameters
const FINGERPRINT_COOKIE_NAME = '_alp_fp';

// Timing and Polling Configuration
const MAX_WAIT_MINUTES = 10;
const POLL_INTERVAL_MS = 30000;

// MAIN EXECUTION FLOW

/**
 * Main function to orchestrate the entire code export process.
 */
async function main() {
    console.log('--- Script Start: Code Exporter Initialization ---');

    // Step 1: Initialize file system and browser resources
    createDirectoryIfMissing(BASE_ASSET_DIR);

    let browser, page;
    try {
        // Initialize the browser instance
        ({ browser, page } = await initializeBrowser());

        // Step 2: Authentication and Setup
        console.log('\n--- Phase 1: Authentication and Region Discovery ---');
        // Fetch the required authentication cookie
        // FIX: Replaced arbitrary wait with condition-based wait inside fetchFingerprintCookie
        const fingerprintCookie = await fetchFingerprintCookie(page);

        // Step 3: Fetch all regions that need processing
        const regionsApiUrl = `${API_DOMAIN}${REGIONS_API_PATH}`;
        const regionSlugs = await fetchAllRegionSlugs(page, regionsApiUrl, fingerprintCookie);
        console.log(`[Phase 1 Complete] Found ${regionSlugs.length} regions to process.`);

        // Step 4: Iterate through each region
        console.log('\n--- Phase 2: Client and Version Identification ---');
        for (const regionSlug of regionSlugs) {
            await processRegion(page, regionSlug, fingerprintCookie);
        }

        console.log('\n-------------------------------------');
        console.log('✓ Script Complete: All available region exports processed! 🎉');
        console.log('-------------------------------------');

    } catch (err) {
        // This catch block handles fatal setup errors
        console.error('\n!!! FATAL SCRIPT ERROR (Browser/Setup) !!!');
        console.error('Error details:', err.message);
        process.exit(1);
    } finally {
        // Step 5: Clean up by closing the browser
        if (browser) {
            await browser.close();
            console.log('\n--- Script End: Browser closed ---');
        }
    }
}

// REGION AND CLIENT PROCESSING

/**
 * Processes all clients within a single region.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {string} regionSlug - The slug identifier for the region.
 * @param {string} fingerprintCookie - The authentication fingerprint cookie value.
 */
async function processRegion(page, regionSlug, fingerprintCookie) {
    console.log(`\n\n=== START REGION: ${regionSlug} ===`);

    // Step 1: Define the region's download folder path
    const regionDownloadFolder = path.join(BASE_ASSET_DIR, regionSlug);

    // Step 2: Configure the browser to use the region's download folder
    await setBrowserDownloadFolder(page, regionDownloadFolder);

    // Step 3: Fetch the list of clients for this region from the API
    const regionApiUrl = `${API_DOMAIN}${REGIONS_API_PATH}${regionSlug}/`;
    const regionData = await fetchRegionDetails(page, regionApiUrl, regionSlug, fingerprintCookie);
    if (!regionData) return;

    const clients = regionData.clients || [];
    console.log(`[${regionSlug}] Found ${clients.length} clients.`);

    // Step 4: Process clients in batches to manage concurrency
    const CONCURRENCY_LIMIT = 2; // Maximum number of simultaneous exports
    let clientIndex = 0;

    while (clientIndex < clients.length) {
        // Select the next batch of clients
        const clientsToProcess = clients.slice(clientIndex, clientIndex + CONCURRENCY_LIMIT);

        if (clientsToProcess.length === 0) break;

        const totalBatches = Math.ceil(clients.length / CONCURRENCY_LIMIT);
        const currentBatch = Math.ceil((clientIndex / CONCURRENCY_LIMIT) + 1);

        console.log(`\n[${regionSlug}] 🚀 Starting Batch: ${currentBatch} / ${totalBatches}`);
        console.log(`[${regionSlug}] Processing ${clientsToProcess.length} client(s): ${clientsToProcess.map(c => c.slug).join(' and ')}`);

        // Create and run the Promises for the current batch concurrently
        const exportPromises = clientsToProcess.map(client =>
            processClientExport(page, client, regionSlug, regionDownloadFolder, fingerprintCookie)
        );

        // Wait for ALL jobs in the current batch to finish
        await Promise.all(exportPromises);

        // Update the index to the next batch
        clientIndex += CONCURRENCY_LIMIT;

        // FIX: Removed the arbitrary/mandatory wait after the concurrent batch.
        // The script can proceed immediately to the next batch without an arbitrary pause.
    }

    console.log(`\n=== END REGION: ${regionSlug} ===`);
}

/**
 * Processes a single client's code export from start to finish.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {Object} client - The client object containing slug.
 * @param {string} regionSlug - The slug identifier for the region.
 * @param {string} regionDownloadFolder - The local folder path for downloads.
 * @param {string} fingerprintCookie - The authentication fingerprint cookie value.
 */
async function processClientExport(page, client, regionSlug, regionDownloadFolder, fingerprintCookie) {
    const clientSlug = client.slug;
    if (!clientSlug) return;

    // Step 1: Determine expected filename and check for existing file
    // Format: [client_slug]-[region_slug]-1.txt (e.g., sandpoint-ak-1.txt)
    const exportBaseName = `${clientSlug}-${regionSlug}${FILE_VERSION_SUFFIX}`;
    const finalExportFileName = `${exportBaseName}${OUTPUT_FILE_EXTENSION}`;
    const finalExportFilePath = path.join(regionDownloadFolder, finalExportFileName);

    console.log(`\n--- START CLIENT: ${clientSlug} (Expected File: ${finalExportFileName}) ---`);

    // Check for existing file using robust FS check
    try {
        if (fs.existsSync(finalExportFilePath)) {
            console.log(`[${clientSlug}] File already exists at ${finalExportFilePath}. Skipping client.`);
            return;
        }
    } catch (e) {
        console.error(`[${clientSlug}] Error checking file existence: ${e.message}`);
        // Proceed anyway, assuming file doesn't exist if check failed
    }

    try {
        // Step 2: Fetch client details to find the latest code version UUID
        const clientApiUrl = `${API_DOMAIN}${CLIENT_API_PREFIX}${clientSlug}/`;
        const clientData = await fetchClientDetails(page, clientApiUrl, clientSlug, fingerprintCookie);

        const versions = clientData?.versions || [];
        if (versions.length === 0) {
            console.log(`[${clientSlug}] ⚠️ No code versions found. Skipping.`);
            return;
        }

        const latestVersion = versions[0];
        const latestVersionId = latestVersion.uuid;

        // Step 3: Fetch version details and the Table of Contents (TOC)
        const versionApiUrl = `${API_DOMAIN}${VERSION_API_PREFIX}${latestVersionId}/`;
        // FIX: Correctly pass latestVersionId
        const versionDetails = await fetchVersionDetailsAndTOC(page, versionApiUrl, latestVersionId, fingerprintCookie);

        if (!versionDetails || !versionDetails.toc?.length) {
            console.log(`[${clientSlug}] 🚫 Skipping: Failed to retrieve TOC details.`);
            return;
        }

        // Step 4: Recursively collect ALL nested UUIDs/slugs for the full export scope
        const exportScopeArray = recursivelyCollectExportScope(versionDetails.toc);
        const mainCodeSlug = versionDetails.toc[0].slug;
        const definitiveVersionId = versionDetails.uuid;

        console.log(`[${clientSlug}] Exporting ${exportScopeArray.length} parts of Code: ${mainCodeSlug} (Version ID: ${definitiveVersionId.substring(0, 8)}...)`);

        // Step 5: Submit the export request (Phase 3)
        console.log(`\n[${clientSlug}] --- Phase 3: Submitting Export Request ---`);
        const exportRequestResponse = await submitExportRequest(
            page,
            definitiveVersionId,
            exportScopeArray,
            fingerprintCookie
        );

        if (!exportRequestResponse || !exportRequestResponse.uuid) {
            console.error(`[${clientSlug}] ❌ Failed to submit new export request. Skipping client.`);
            return;
        }

        const exportJobId = exportRequestResponse.uuid;
        console.log(`[${clientSlug}] ✅ New export job submitted. Job ID (UUID): ${exportJobId.substring(0, 8)}...`);

        // Step 6: Wait for Export Completion and Download (Phase 4)
        console.log(`\n[${clientSlug}] --- Phase 4: Waiting for Export and Downloading ---`);
        const isExportSuccessful = await pollForExportCompletion(
            page,
            exportJobId,
            fingerprintCookie
        );

        if (isExportSuccessful) {
            console.log(`[${clientSlug}] 💾 Export task finished successfully. Initiating download...`);
            // Download the file and rename it to the expected final path
            await downloadExportFile(page, exportJobId, finalExportFilePath);
            console.log(`[${clientSlug}] 🎉 Download completed and verified: ${finalExportFileName}`);
        } else {
            console.error(`[${clientSlug}] ⚠️ Export failed or timed out for Job ID: ${exportJobId.substring(0, 8)}...`);
        }

    } catch (clientError) {
        console.error(`[CRITICAL CLIENT ERROR] 🛑 Failure processing client ${clientSlug}. Error:`, clientError.message);
    }
}

// BROWSER AND UTILITY FUNCTIONS

/**
 * Launches a Puppeteer browser instance and creates a new page.
 * @returns {Promise<{browser: puppeteer.Browser, page: puppeteer.Page}>}
 */
async function initializeBrowser() {
    console.log(`[BROWSER] Launching browser (headless: ${BROWSER_HEADLESS_MODE})...`);

    const browser = await puppeteer.launch({
        headless: BROWSER_HEADLESS_MODE,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--start-maximized',
            '--window-size=1920,1080',
        ],
        defaultViewport: null,
    });

    const page = await browser.newPage();
    console.log('[BROWSER] Browser launched and new page created.');
    return { browser, page };
}

/**
 * Configures the Puppeteer page to download files to a specific local folder.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {string} folderPath - The local path to set as the download directory.
 * @returns {Promise<void>}
 */
async function setBrowserDownloadFolder(page, folderPath) {
    createDirectoryIfMissing(folderPath);
    const resolvedPath = path.resolve(folderPath);
    const client = await page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', {
        behavior: 'allow',
        downloadPath: resolvedPath
    });
    console.log(`[BROWSER] Download folder set to: ${resolvedPath}`);
}

/**
 * Navigates to the base URL to fetch the essential fingerprint cookie for authorization.
 * FIX: Replaced arbitrary wait with a condition-based poll for the cookie.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @returns {Promise<string>} The value of the fingerprint cookie.
 */
async function fetchFingerprintCookie(page) {
    const targetUrl = API_DOMAIN;
    const cookiePollInterval = 500; // Check every 0.5 seconds
    const maxCookieWaitMs = 15000; // Max wait 15 seconds

    try {
        console.log(`[AUTH] 🌐 Visiting URL: ${targetUrl} to get authentication cookie...`);
        await page.goto(targetUrl, { waitUntil: 'networkidle2', timeout: NAVIGATION_TIMEOUT_MS });

        let fingerprintCookie = null;
        const startTime = Date.now();

        console.log(`[AUTH] Polling for cookie "${FINGERPRINT_COOKIE_NAME}" (max ${maxCookieWaitMs / 1000}s)...`);

        while (Date.now() - startTime < maxCookieWaitMs) {
            const cookies = await page.cookies();
            fingerprintCookie = cookies.find(c => c.name === FINGERPRINT_COOKIE_NAME);
            if (fingerprintCookie) break;

            // Wait for a short interval before checking again
            await politeWaitSimple(cookiePollInterval);
        }

        if (!fingerprintCookie) {
            throw new Error(`Authentication cookie "${FINGERPRINT_COOKIE_NAME}" not found after ${maxCookieWaitMs / 1000}s.`);
        }

        console.log(`[AUTH] ✅ Retrieved authentication cookie.`);
        return fingerprintCookie.value;
    } catch (err) {
        console.error(`[AUTH] ❌ Critical error retrieving authentication cookie.`);
        throw err;
    }
}

/**
 * Creates a directory recursively if it doesn't exist.
 * @param {string} directoryPath - The path to the directory.
 * @returns {void}
 */
function createDirectoryIfMissing(directoryPath) {
    // Added try...catch for safer FS operation (Improvement 3)
    try {
        if (!fs.existsSync(directoryPath)) {
            console.log(`[UTIL] Creating directory: ${directoryPath}`);
            fs.mkdirSync(directoryPath, { recursive: true });
        }
    } catch (error) {
        console.error(`[UTIL] Failed to create directory ${directoryPath}: ${error.message}`);
    }
}

/**
 * Pauses execution for a specified duration. (Used for longer, logging waits)
 * @param {number} ms - The duration in milliseconds.
 * @returns {Promise<void>}
 */
async function politeWait(ms) {
    console.log(`[UTIL] Pausing for ${ms / 1000} seconds...`);
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Pauses execution for a specified duration. (Used for short, non-logged internal waits)
 * @param {number} ms - The duration in milliseconds.
 * @returns {Promise<void>}
 */
async function politeWaitSimple(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// API COMMUNICATION FUNCTIONS

/**
 * Performs a non-navigating GET request within the browser's context.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {string} url - The API endpoint URL.
 * @param {string} fingerprintValue - The authentication fingerprint cookie value.
 * @returns {Promise<Object|null>} The parsed JSON data or null on failure.
 */
async function executeGetRequest(page, url, fingerprintValue) {
    try {
        console.log(`[API_GET] 🌐 Sending GET request to: ${url}`);

        const response = await page.evaluate(
            async (apiUrl, fingerprint, timeout) => {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeout);

                try {
                    const res = await fetch(apiUrl, {
                        method: 'GET',
                        headers: {
                            'Content-Type': 'application/json',
                            'Fingerprint': fingerprint
                        },
                        signal: controller.signal
                    });
                    clearTimeout(timeoutId);

                    if (!res.ok) {
                        return { status: res.status, data: `HTTP error! status: ${res.status}` };
                    }
                    return { status: res.status, data: await res.text() };
                } catch (error) {
                    clearTimeout(timeoutId);
                    return { status: 0, data: `Request failed or timed out: ${error.message}` };
                }
            },
            url, fingerprintValue, NAVIGATION_TIMEOUT_MS
        );

        if (response.status >= 200 && response.status < 300) {
            console.log(`[API_GET] ✅ Success (${response.status}) from ${url}.`);
            return JSON.parse(response.data);
        } else {
            console.error(`[API_GET] ❌ Request failed. Status: ${response.status}. Response: ${response.data}`);
            return null;
        }

    } catch (err) {
        console.error(`[API_GET] ❌ Error executing GET request to ${url}: ${err.message}`);
        return null;
    }
}

/**
 * Submits a new export request (POST) and receives the Job ID (UUID).
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {string} versionUuid - The UUID of the code version to export.
 * @param {Array<Object>} scopeArray - An array of UUID/slug objects defining the export scope.
 * @param {string} fingerprintValue - The authentication fingerprint cookie value.
 * @returns {Promise<Object|null>} The parsed JSON response containing the job UUID.
 */
async function submitExportRequest(page, versionUuid, scopeArray, fingerprintValue) {
    try {
        const exportApiUrl = `${API_DOMAIN}${EXPORT_API_PATH}`;
        const requestPayload = {
            version: versionUuid,
            scope: JSON.stringify(scopeArray),
            output_format: 'txt',
            for_print: false
        };

        console.log(`[EXPORT] 📤 Sending Payload: Version=${versionUuid.substring(0, 8)}... | Scope Parts=${scopeArray.length}`);
        console.log(`[EXPORT] 🌐 Sending POST request to: ${exportApiUrl}`);

        const response = await page.evaluate(
            async (url, payload, fingerprint, timeout) => {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeout);

                try {
                    const res = await fetch(url, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Fingerprint': fingerprint
                        },
                        body: JSON.stringify(payload),
                        signal: controller.signal
                    });
                    clearTimeout(timeoutId);
                    return { status: res.status, data: await res.text() };
                } catch (error) {
                    clearTimeout(timeoutId);
                    return { status: 0, data: `Request failed or timed out: ${error.message}` };
                }
            },
            exportApiUrl, requestPayload, fingerprintValue, NAVIGATION_TIMEOUT_MS
        );

        if (response.status === 201) {
            return JSON.parse(response.data);
        } else {
            console.error(`[EXPORT] ❌ Request failed. Status: ${response.status}. Response: ${response.data}`);
            return null;
        }
    } catch (err) {
        console.error(`[EXPORT] ❌ Error submitting export request: ${err.message}`);
        return null;
    }
}

/**
 * Fetches the list of all export requests to check the status of a specific job.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {string} fingerprintValue - The authentication fingerprint cookie value.
 * @returns {Promise<Array<Object>|null>} An array of export job objects.
 */
async function checkAllExportStatuses(page, fingerprintValue) {
    try {
        const statusUrl = `${API_DOMAIN}${EXPORT_API_PATH}`;
        const response = await page.evaluate(
            async (url, fingerprint, timeout) => {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeout);

                try {
                    const res = await fetch(url, {
                        method: 'GET',
                        headers: { 'Fingerprint': fingerprint },
                        signal: controller.signal
                    });
                    clearTimeout(timeoutId);
                    return await res.text();
                } catch (error) {
                    clearTimeout(timeoutId);
                    throw new Error(`Status check failed: ${error.message}`);
                }
            },
            statusUrl, fingerprintValue, NAVIGATION_TIMEOUT_MS
        );
        return JSON.parse(response);
    } catch (err) {
        console.error(`[STATUS] ❌ Error checking export status: ${err.message}`);
        return null;
    }
}

/**
 * Polls the export status API until the target job completes or times out.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {string} exportJobId - The UUID of the export job to monitor.
 * @param {string} fingerprintValue - The authentication fingerprint cookie value.
 * @returns {Promise<boolean>} True if successful, false otherwise.
 */
async function pollForExportCompletion(page, exportJobId, fingerprintValue) {
    const maxAttempts = MAX_WAIT_MINUTES * (60000 / POLL_INTERVAL_MS);
    const shortJobId = exportJobId.substring(0, 8);
    console.log(`[STATUS: ${shortJobId}] ⏳ Starting poll (max ${MAX_WAIT_MINUTES} min / ${maxAttempts} attempts)...`);

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        await politeWait(POLL_INTERVAL_MS);

        const exportsList = await checkAllExportStatuses(page, fingerprintValue);
        if (!Array.isArray(exportsList)) continue;

        const targetExport = exportsList.find(e => e.uuid === exportJobId);
        if (!targetExport) {
            console.log(`[STATUS: ${shortJobId}] Attempt ${attempt}/${maxAttempts}. Job status not yet available. Retrying...`);
            continue;
        }

        const taskState = targetExport.task?.post_state;
        const progress = targetExport.task?.progress || 0;

        if (taskState === 'SUCCESS') {
            console.log(`[STATUS: ${shortJobId}] ✅ Completed successfully.`);
            return true;
        }

        if (taskState === 'FAILURE') {
            console.error(`[STATUS: ${shortJobId}] ❌ Failed. State: FAILURE.`);
            return false;
        }

        console.log(`[STATUS: ${shortJobId}] Attempt ${attempt}/${maxAttempts}. Progress: ${progress}% (${taskState || 'PENDING'})...`);
    }

    console.warn(`[STATUS: ${shortJobId}] ⚠️ Did not complete within ${MAX_WAIT_MINUTES} minutes. Timeout reached.`);
    return false;
}

// --- Specific API Wrappers ---

/** Fetches a list of all region slugs. */
async function fetchAllRegionSlugs(page, apiUrl, fingerprintCookie) {
    console.log(`[REGION] 🌐 Fetching all region slugs from API: ${apiUrl}`);
    const regionsData = await executeGetRequest(page, apiUrl, fingerprintCookie);
    return regionsData?.filter(r => r.slug).map(r => r.slug) || [];
}

/** Fetches details for a specific region (client list). */
async function fetchRegionDetails(page, apiUrl, regionSlug, fingerprintCookie) {
    console.log(`[REGION] 🌐 Fetching region details for ${regionSlug}...`);
    return executeGetRequest(page, apiUrl, fingerprintCookie);
}

/** Fetches details for a specific client (code version list). */
async function fetchClientDetails(page, apiUrl, clientSlug, fingerprintCookie) {
    console.log(`[CLIENT] 🌐 Fetching client details for ${clientSlug}...`);
    return executeGetRequest(page, apiUrl, fingerprintCookie);
}

/** Fetches the specific code version details and its Table of Contents (TOC). */
async function fetchVersionDetailsAndTOC(page, apiUrl, versionId, fingerprintCookie) {
    console.log(`[VERSION] 🌐 Fetching details for version ${versionId.substring(0, 8)}...`);
    return executeGetRequest(page, apiUrl, fingerprintCookie);
}

// DOWNLOAD AND FILE MANAGEMENT

/**
 * Initiates the browser download, waits for completion, and renames the file to the final path.
 * @param {puppeteer.Page} page - The Puppeteer page instance.
 * @param {string} exportJobId - The UUID of the export job.
 * @param {string} saveFilePath - The final, desired local path for the downloaded file.
 * @returns {Promise<boolean>} True if download and rename succeeded, false otherwise.
 */
async function downloadExportFile(page, exportJobId, saveFilePath) {
    const regionDownloadFolder = path.dirname(saveFilePath);
    const finalExportFileName = path.basename(saveFilePath);
    let tempFilePath;

    try {
        // Step 1: Record existing files to identify the new download (Improvement 3)
        const filesBeforeDownload = new Set(getDirectoryFiles(regionDownloadFolder));

        // Step 2: Navigate to the download URL to trigger the file transfer
        const downloadUrl = `${DOWNLOAD_DOMAIN}${EXPORT_API_PATH}${exportJobId}/download/`;
        console.log(`[DOWNLOAD] 🌐 Visiting final download URL: ${downloadUrl}`);
        await page.goto(downloadUrl, { waitUntil: 'networkidle2', timeout: 300000 });

        // Step 3: Wait for the download process to finish (no more temporary files)
        console.log(`[DOWNLOAD] Waiting for file system to register and complete download...`);
        // The original waitForDownloadCompletion is robust and necessary here.
        await waitForDownloadCompletion(regionDownloadFolder, 60000);

        // Step 4: Identify the newly downloaded file
        const allFilesAfterDownload = getDirectoryFiles(regionDownloadFolder);
        let actualDownloadedFileName = allFilesAfterDownload.find(file => !filesBeforeDownload.has(file));

        if (!actualDownloadedFileName) {
            // Fallback: use the absolute newest file if the comparison fails
            actualDownloadedFileName = getNewestFile(regionDownloadFolder);
            if (!actualDownloadedFileName) {
                throw new Error('Could not identify the newly downloaded file after completion.');
            }
            console.warn(`[DOWNLOAD] Fallback: Used newest file heuristic: ${actualDownloadedFileName}`);
        }

        tempFilePath = path.join(regionDownloadFolder, actualDownloadedFileName);
        const finalFilePath = saveFilePath;

        // Step 5: Rename the completed download file to the final target name
        try { // Improved FS safety (Improvement 3)
            if (fs.existsSync(finalFilePath)) {
                fs.unlinkSync(finalFilePath); // Delete old file before renaming
            }
            // Rename the file to the expected final name
            fs.renameSync(tempFilePath, finalFilePath);
        } catch (e) {
            throw new Error(`Failed to rename file from ${path.basename(tempFilePath)} to ${finalExportFileName}: ${e.message}`);
        }

        console.log(`[DOWNLOAD] ✅ Download complete. Renamed to final file: ${finalExportFileName}`);
        return true;
    } catch (err) {
        console.error(`[DOWNLOAD] ❌ Error downloading job ID ${exportJobId.substring(0, 8)}...: ${err.message}`);

        // Clean up any partially downloaded temp file
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            console.log(`[DOWNLOAD] Cleaning up temporary file: ${path.basename(tempFilePath)}`);
            fs.unlinkSync(tempFilePath);
        }
        return false;
    }
}

/**
 * Helper function to safely read directory contents, filtering out temp files.
 * @param {string} directoryPath - The path to the directory.
 * @returns {Array<string>} List of file names.
 */
function getDirectoryFiles(directoryPath) {
    const tempExtensions = ['.tmp', '.crdownload', '.part'];
    try {
        return fs.readdirSync(directoryPath).filter(file =>
            !tempExtensions.some(ext => file.toLowerCase().endsWith(ext))
        );
    } catch (e) {
        console.error(`[UTIL] Error reading directory ${directoryPath}: ${e.message}`);
        return [];
    }
}

/**
 * Polls the local file system until the download process has completed (no temporary files).
 * @param {string} directoryPath - The path to the download directory.
 * @param {number} timeoutMs - The maximum time to wait in milliseconds.
 * @returns {Promise<boolean>} Resolves true when no temp files are found.
 */
async function waitForDownloadCompletion(directoryPath, timeoutMs = 60000) {
    const pollInterval = 1000;
    const maxAttempts = Math.ceil(timeoutMs / pollInterval);
    let attempts = 0;
    const tempExtensions = ['.tmp', '.crdownload', '.part'];

    return new Promise((resolve, reject) => {
        const interval = setInterval(() => {
            attempts++;

            try {
                // Check if any temporary download file exists
                const files = fs.readdirSync(directoryPath);
                const isDownloading = files.some(file =>
                    tempExtensions.some(ext => file.toLowerCase().endsWith(ext))
                );

                if (!isDownloading) {
                    clearInterval(interval);
                    resolve(true);
                    return;
                }
            } catch (e) {
                // Handle file system errors during polling (Improvement 3)
                clearInterval(interval);
                reject(new Error(`File system error during download wait: ${e.message}`));
                return;
            }

            if (attempts >= maxAttempts) {
                clearInterval(interval);
                reject(new Error(`File download did not complete within ${timeoutMs / 1000} seconds.`));
            }
        }, pollInterval);
    });
}

/**
 * Gets the newest non-temporary file in a directory based on its modification time.
 * @param {string} directoryPath - The directory to check.
 * @returns {string|null} The filename of the newest non-temp file.
 */
function getNewestFile(directoryPath) {
    try {
        const files = getDirectoryFiles(directoryPath); // Use safe helper
        if (files.length === 0) return null;

        let newestFile = null;
        let newestTime = 0;

        for (const file of files) {
            const filePath = path.join(directoryPath, file);
            // Added try/catch for statSync (Improvement 3)
            let stat;
            try {
                stat = fs.statSync(filePath);
            } catch (e) {
                console.warn(`[UTIL] Skipping file ${file}: ${e.message}`);
                continue;
            }

            if (stat.mtimeMs > newestTime) {
                newestTime = stat.mtimeMs;
                newestFile = file;
            }
        }
        return newestFile;
    } catch (error) {
        console.error(`[UTIL] Error reading directory for newest file: ${error.message}`);
        return null;
    }
}

// EXPORT SCOPE UTILITY

/**
 * Recursively traverses a nested Table of Contents (TOC) structure
 * and collects all UUIDs and slugs for the full export scope.
 * @param {Array<Object>} tocArray - The array of TOC items.
 * @param {Array<Object>} scope - The current collection of UUID/slug objects.
 * @returns {Array<Object>} The complete list of objects for the export scope.
 */
function recursivelyCollectExportScope(tocArray, scope = []) {
    if (!Array.isArray(tocArray)) return scope;

    for (const item of tocArray) {
        // Step 1: Add the current item's UUID and slug
        if (item.uuid && item.slug) {
            scope.push({
                uuid: item.uuid,
                code_slug: item.slug
            });
        }
        // Step 2: Recursively check for nested children
        if (item.children && Array.isArray(item.children)) {
            recursivelyCollectExportScope(item.children, scope);
        }
    }
    return scope;
}


// EXECUTION

// Call the main function and handle any top-level errors
main().catch(err => {
    console.error('Fatal error outside main execution block:', err);
    process.exit(1);
});
