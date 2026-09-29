import logger from '../utils/logger.js';

// Next bundles API routes separately; share ownership with the stop endpoint.
const operations = globalThis.scraperOperations ??= new Set();
const CLOSE_TIMEOUT_MS = 5000;

function cancellationError() {
    return Object.assign(new Error('Scraping cancelled by user'), { code: 'SCRAPE_CANCELLED' });
}

export async function stopOwnedScrapers() {
    const active = [...operations];
    for (const operation of active) operation.cancel();
    await Promise.all(active.map(operation => operation.finish()));
}

export function createScraperLifecycle(scraper, { deferTermination, checkCancelled } = {}) {
    const terminate = scraper.terminate.bind(scraper);
    let ended = false;
    let reason;
    let rejectCancelled;
    let finishing;
    let ownedBrowser;
    let polling;
    const cancelled = new Promise((_, reject) => { rejectCancelled = reject; });
    // Cancellation can arrive before the caller installs its race handler.
    cancelled.catch(() => {});

    async function boundedCleanup(cleanup, browser = ownedBrowser) {
        let timer;
        try {
            await Promise.race([
                Promise.resolve().then(cleanup),
                new Promise((_, reject) => {
                    timer = setTimeout(() => reject(new Error('Scraper cleanup timed out')), CLOSE_TIMEOUT_MS);
                }),
            ]);
        } catch (error) {
            logger.warn({ error: error.message }, '[Scraper] Cleanup failed');
        } finally {
            clearTimeout(timer);
            // Only a browser launched by this scraper may be force-stopped.
            // This also handles the library swallowing a browser.close() error.
            const child = browser?.process();
            if (child && child.exitCode === null && child.signalCode === null) {
                try { child.kill('SIGKILL'); } catch (error) {
                    logger.warn({ error: error.message }, '[Scraper] Owned browser process could not be stopped');
                }
            }
        }
    }

    const operation = {
        cancelled,
        cancel(error = cancellationError()) {
            if (ended || reason) return;
            reason = error;
            rejectCancelled(error);
        },
        throwIfCancelled() {
            if (!reason && checkCancelled?.()) operation.cancel();
            if (reason) throw reason;
            if (ended) throw cancellationError();
        },
        finish() {
            if (!finishing) {
                ended = true;
                clearInterval(polling);
                finishing = boundedCleanup(() => terminate(true)).finally(() => operations.delete(operation));
            }
            return finishing;
        },
    };

    // Called after launch but before creating the page. If launch outlives the
    // timeout, close its late browser here before any login/navigation occurs.
    if (scraper.options) {
        const prepareBrowser = scraper.options.prepareBrowser;
        scraper.options.prepareBrowser = async browser => {
            ownedBrowser = browser;
            if (ended || reason) {
                await boundedCleanup(() => browser.close(), browser);
                operation.throwIfCancelled();
            }
            if (prepareBrowser) await prepareBrowser(browser);
            operation.throwIfCancelled();
        };
    }

    scraper.terminate = async success => {
        if (ended) {
            // initialize() can still have registered a page after finalization.
            await boundedCleanup(() => terminate(true));
        } else if (!deferTermination) {
            await terminate(success);
        }
    };

    // The upstream initialize() is outside its try/finally. Clean resources
    // registered after a timeout even if initialization then rejects.
    if (scraper.initialize) {
        const initialize = scraper.initialize.bind(scraper);
        scraper.initialize = async (...args) => {
            operation.throwIfCancelled();
            try {
                const result = await initialize(...args);
                operation.throwIfCancelled();
                return result;
            } finally {
                if (ended || reason) await boundedCleanup(() => terminate(true));
            }
        };
    }

    operations.add(operation);
    if (checkCancelled) {
        polling = setInterval(() => {
            try { operation.throwIfCancelled(); } catch (error) { operation.cancel(error); }
        }, 250);
    }
    return operation;
}
