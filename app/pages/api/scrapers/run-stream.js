import { getDB } from '../db';
import { BANK_VENDORS } from '../../../utils/constants';
import logger from '../../../utils/logger.js';
import {
  prepareCredentials,
  validateCredentials,
  getScraperOptions,
  insertScrapeAudit,
  updateScrapeAudit,
  updateCredentialLastSynced,
  getFetchCategoriesSetting,
  getScraperTimeout,
  getScrapeRetries,
  runScraper,
  loadCategorizationRules,
  loadCategoryMappings,
  getUpdateCategoryOnRescrapeSetting,
  getLogHttpRequestsSetting,
  getBillingCycleStartDay,
  processScrapedAccounts,
  checkScraperConcurrency,
} from '../utils/scraperUtils';
import { VaultLockedError, decrypt, encrypt } from '../utils/encryption';
import { classifyScrapeError, ScrapeErrorTypes } from '../utils/scraperErrors';

const CompanyTypes = {
  hapoalim: 'hapoalim',
  leumi: 'leumi',
  discount: 'discount',
  otsarHahayal: 'otsarHahayal',
  mercantile: 'mercantile',
  mizrahi: 'mizrahi',
  igud: 'igud',
  massad: 'massad',
  yahav: 'yahav',
  beinleumi: 'beinleumi',
  onezero: 'onezero',
  isracard: 'isracard',
  amex: 'amex',
  max: 'max',
  visaCal: 'visaCal',
};

// Helper to send SSE messages to the local client
function sendSSE(res, event, data) {
  if (res && !res.destroyed && !res.finished && !res.writableEnded) {
    try {
      res.write(`event: ${event}\n`);
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    } catch (err) {
      // Ignore if client disconnected
    }
  }
}

async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ message: 'Method not allowed' });
  }

  // Set up SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const client = await getDB();
  const startTime = Date.now();
  let auditId = null;

  try {
    // Check for other running scrapers
    try {
      await checkScraperConcurrency(client);
    } catch (concurrencyError) {
      logger.warn({ error: concurrencyError.message }, '[Scrape Stream] Concurrency check failed');
      sendSSE(res, 'error', {
        message: concurrencyError.message,
        type: 'CONCURRENCY_ERROR'
      });
      res.end();
      return;
    }

    const { options, credentials, credentialId } = req.body;
    const companyId = CompanyTypes[options.companyId];

    if (!companyId) {
      sendSSE(res, 'error', { message: 'Invalid company ID' });
      res.end();
      return;
    }

    logger.info({ vendor: options.companyId, credentialId, startDate: options.startDate, showBrowser: options.showBrowser }, '[Scrape Stream] Starting scrape');

    sendSSE(res, 'progress', {
      step: 'init',
      message: `Initializing scraper for ${options.companyId}...`,
      percent: 0,
      phase: 'initialization',
      success: null
    });

    const isBank = BANK_VENDORS.includes(options.companyId);

    // OneZero: load the long-term OTP token from the DB (encrypted at rest, never travels in HTTP).
    // If present, we'll skip the SMS round-trip; if absent, the scraper invokes our otpCodeRetriever.
    let onezeroEnrichedCredentials = credentials;
    if (options.companyId === 'onezero' && credentialId) {
      try {
        const tokenRow = await client.query(
          'SELECT otp_long_term_token FROM vendor_credentials WHERE id = $1',
          [credentialId]
        );
        const ciphertext = tokenRow.rows[0]?.otp_long_term_token;
        if (ciphertext) {
          onezeroEnrichedCredentials = {
            ...credentials,
            otpLongTermToken: decrypt(ciphertext)
          };
          logger.info({ credentialId }, '[Scrape Stream] OneZero: using stored long-term OTP token');
        } else {
          logger.info({ credentialId }, '[Scrape Stream] OneZero: no stored token, will request SMS OTP');
        }
      } catch (err) {
        logger.warn({ error: err.message }, '[Scrape Stream] OneZero: failed to load stored token, falling back to SMS OTP');
      }
    }

    // Prepare and validate credentials
    const scraperCredentials = prepareCredentials(options.companyId, onezeroEnrichedCredentials);

    try {
      validateCredentials(scraperCredentials, options.companyId);
    } catch (error) {
      logger.error({ error: error.message }, '[Scrape Stream] Credential validation failed');
      sendSSE(res, 'error', { message: error.message });
      res.end();
      return;
    }


    // Show date range being scraped
    const startDateStr = new Date(options.startDate).toLocaleDateString('en-GB');
    const todayStr = new Date().toLocaleDateString('en-GB');
    sendSSE(res, 'progress', {
      step: 'date_range',
      message: `📅 Scraping from ${startDateStr} to ${todayStr}`,
      percent: 4,
      phase: 'initialization',
      success: null
    });

    sendSSE(res, 'progress', {
      step: 'browser',
      message: 'Launching browser...',
      percent: 5,
      phase: 'initialization',
      success: null
    });

    // Get settings from database
    const showBrowserSetting = options.showBrowser || false;
    const fetchCategoriesSetting = await getFetchCategoriesSetting(client);
    const timeoutSetting = await getScraperTimeout(client, companyId);
    const updateCategoryOnRescrape = await getUpdateCategoryOnRescrapeSetting(client);
    const logHttpRequests = await getLogHttpRequestsSetting(client);

    // Build scraper options with progress callback
    const scraperOptions = {
      ...getScraperOptions(companyId, new Date(options.startDate), {
        timeout: timeoutSetting,
        defaultTimeout: timeoutSetting,
        showBrowser: showBrowserSetting,
        fetchCategories: fetchCategoriesSetting,
      }),
      logRequests: logHttpRequests,
    };

    // Track completed steps for better status reporting
    const completedSteps = new Set();

    const progressHandler = (companyId, payload) => {
      if (companyId === 'network') {
        sendSSE(res, 'network', payload);
        return;
      }

      if (payload?.type === 'screenshot') {
        sendSSE(res, 'screenshot', payload);
        return;
      }

      const stepMessages = {
        'initializing': { message: 'Initializing scraper...', percent: 5, phase: 'initialization', success: true },
        'startScraping': { message: 'Starting scrape process...', percent: 10, phase: 'initialization', success: true },
        'loginStarted': { message: 'Navigating to login page...', percent: 20, phase: 'authentication', success: null },
        'loginWaitingForOTP': { message: 'Waiting for OTP verification...', percent: 25, phase: 'authentication', success: null },
        'loginSuccess': { message: '✓ Login successful', percent: 35, phase: 'authentication', success: true },
        'loginFailed': { message: '✗ Login failed', percent: 35, phase: 'authentication', success: false },
        'changePassword': { message: 'Password change required', percent: 30, phase: 'authentication', success: false },
        'otpRequired': { message: '🔐 2FA verification required - enter the SMS code sent to your phone', percent: 25, phase: 'authentication', success: null },
        'otpSubmitting': { message: 'Submitting verification code...', percent: 28, phase: 'authentication', success: null },
        'otpSuccess': { message: '✓ 2FA verification successful', percent: 32, phase: 'authentication', success: true },
        'otpFailed': { message: '✗ 2FA verification failed', percent: 30, phase: 'authentication', success: false },
        'fetchingTransactions': { message: 'Fetching transactions from website...', percent: 45, phase: 'data_fetching', success: null },
        'gettingAccountDetails': { message: 'Retrieving account details...', percent: 50, phase: 'data_fetching', success: null },
        'accountDetailsReceived': { message: '✓ Account details received', percent: 55, phase: 'data_fetching', success: true },
        'processingAccount': { message: `Processing account ${payload?.accountNumber || ''}...`, percent: 60, phase: 'processing', success: null },
        'processingTransactions': { message: 'Processing transactions...', percent: 65, phase: 'processing', success: null },
        'fetchingCategory': { message: 'Fetching transaction category...', percent: 70, phase: 'processing', success: null },
        'endScraping': { message: '✓ Scraping completed', percent: 75, phase: 'processing', success: true }
      };

      const stepInfo = stepMessages[payload?.type] || { message: `${payload?.type || 'Processing'}...`, percent: 50, phase: 'processing', success: null };

      if (stepInfo.success !== null) {
        completedSteps.add(payload?.type);
      }

      sendSSE(res, 'progress', {
        step: payload?.type || 'unknown',
        message: stepInfo.message,
        percent: stepInfo.percent,
        phase: stepInfo.phase,
        success: stepInfo.success,
        completedSteps: Array.from(completedSteps),
        details: payload
      });
    };

    // Insert audit row
    const triggeredBy = credentials?.username || credentials?.id || credentials?.nickname || 'unknown';
    auditId = await insertScrapeAudit(client, triggeredBy, options.companyId, new Date(options.startDate));

    sendSSE(res, 'progress', {
      step: 'scraping',
      message: 'Connecting to bank/credit card website...',
      percent: 15,
      phase: 'initialization',
      success: null
    });

    // Track client connection
    let clientDisconnected = false;
    res.on('close', () => {
      clientDisconnected = true;
      logger.info({ vendor: options.companyId }, '[Scrape Stream] Client disconnected, continuing in background');
    });

    const accumulatedStats = {
      accounts: 0,
      transactions: 0,
      savedTransactions: 0,
      duplicateTransactions: 0,
      updatedTransactions: 0,
      bankTransactions: 0,
      cachedCategories: 0,
      ruleCategories: 0,
      scraperCategories: 0,
      skippedCards: 0,
      processedTransactions: []
    };

    // Get retry settings
    const maxRetries = await getScrapeRetries(client);
    logger.info({ maxRetries }, '[Scrape Stream] Retry settings loaded');

    let result;
    let lastError = null;
    let attempt = 0;

    // Retry loop with UI feedback
    for (attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          // Calculate exponential backoff: 5s, 10s, 20s, etc.
          const retryDelay = Math.min(5000 * Math.pow(2, attempt - 1), 60000);
          logger.info({
            attempt,
            maxRetries,
            retryDelay,
            previousError: lastError
          }, '[Scrape Stream] Retrying scrape after delay');

          await updateScrapeAudit(client, auditId, 'started', `Retry attempt ${attempt}/${maxRetries} after ${retryDelay}ms`);

          // Send network event for countdown timer in UI
          sendSSE(res, 'network', {
            type: 'retryWait',
            message: `Retrying in ${retryDelay / 1000}s (retry ${attempt}/${maxRetries})...`,
            seconds: retryDelay / 1000,
            timestamp: new Date().toISOString()
          });

          // Send retry message to UI
          sendSSE(res, 'progress', {
            step: 'retry',
            message: `⏳ Retrying scrape in ${retryDelay / 1000}s (retry ${attempt}/${maxRetries})...`,
            percent: 15,
            phase: 'retry',
            success: null,
            attemptNumber: attempt,
            maxRetries: maxRetries,
            retryDelay: retryDelay
          });

          await new Promise(resolve => setTimeout(resolve, retryDelay));

          // Clear retry wait state in UI
          sendSSE(res, 'network', {
            type: 'rateLimitFinished',
            timestamp: new Date().toISOString()
          });

          // Update message after wait
          sendSSE(res, 'progress', {
            step: 'retryStart',
            message: `🔄 Starting retry ${attempt}/${maxRetries}...`,
            percent: 20,
            phase: 'retry',
            success: null,
            attemptNumber: attempt,
            maxRetries: maxRetries
          });
        } else {
          logger.info({ companyId: options.companyId }, '[Scrape Stream] Starting initial scrape attempt');
        }

        sendSSE(res, 'progress', {
          step: 'startScraping',
          message: 'Starting scrape process...',
          percent: 10,
          phase: 'initialization',
          success: true
        });

        result = await runScraper(client, scraperOptions, scraperCredentials, progressHandler, () => false);

        // OneZero: if the scraper minted a fresh long-term token (first login or after expiry),
        // persist it encrypted so subsequent syncs skip the SMS step.
        if (options.companyId === 'onezero' && result?.persistentOtpToken && credentialId) {
          try {
            await client.query(
              'UPDATE vendor_credentials SET otp_long_term_token = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
              [encrypt(result.persistentOtpToken), credentialId]
            );
            logger.info({ credentialId }, '[Scrape Stream] OneZero: persisted long-term OTP token');
          } catch (err) {
            logger.warn({ error: err.message, credentialId }, '[Scrape Stream] OneZero: failed to persist long-term OTP token');
          }
        }

        if (!result.success) {
          // OTP-specific short-circuit: browser session is bound to this attempt.
          if (result.otpPending) {
            logger.info('[Scrape Stream] OTP flow was triggered but failed - not retrying');
            const classified = classifyScrapeError({ libResult: result, capturedResponse: result.capturedResponse });
            const otpType = classified.type === ScrapeErrorTypes.UNKNOWN ? ScrapeErrorTypes.OTP_FAILED : classified.type;
            if (auditId) {
              await updateScrapeAudit(client, auditId, 'failed', result.errorMessage || 'OTP verification failed');
            }
            if (!res.finished) {
              sendSSE(res, 'error', {
                type: otpType,
                message: classified.userMessage,
                originalMessage: result.errorMessage || classified.originalMessage,
                retryable: false,
                attemptsMade: attempt + 1,
              });
              res.end();
            }
            return;
          }
          const failure = new Error(result.errorMessage || 'Scraper failed');
          failure._libResult = result;
          failure.capturedResponse = result.capturedResponse;
          throw failure;
        }

        // Success - break out of retry loop
        if (attempt > 0) {
          logger.info({ attempt }, '[Scrape Stream] Scrape succeeded after retry');
          sendSSE(res, 'progress', {
            step: 'retrySuccess',
            message: `✓ Retry successful on attempt ${attempt + 1}!`,
            percent: 20,
            phase: 'retry',
            success: true,
            attemptNumber: attempt
          });
        }
        break;

      } catch (scrapeError) {
        if (scrapeError.code === 'SCRAPE_CANCELLED') {
          sendSSE(res, 'error', { type: 'CANCELLED', message: scrapeError.message, retryable: false });
          if (!res.finished) res.end();
          return;
        }
        lastError = scrapeError.message;
        const classified = classifyScrapeError({
          thrownError: scrapeError,
          libResult: scrapeError._libResult,
          capturedResponse: scrapeError.capturedResponse,
        });
        logger.error({
          attempt,
          maxRetries,
          error: scrapeError.message,
          classifiedType: classified.type,
          retryable: classified.retryable,
        }, '[Scrape Stream] Scrape attempt failed');

        const isFinalAttempt = attempt >= maxRetries;
        const giveUp = isFinalAttempt || !classified.retryable;

        if (giveUp) {
          if (!classified.retryable) {
            logger.info({ classifiedType: classified.type }, '[Scrape Stream] Not retrying — error classified as non-retryable');
          } else {
            logger.error({
              totalAttempts: attempt + 1,
              maxRetries,
            }, '[Scrape Stream] All retry attempts exhausted');
          }

          if (auditId) {
            await updateScrapeAudit(
              client,
              auditId,
              'failed',
              `[${classified.type}] ${classified.originalMessage || scrapeError.message}`,
              null,
              attempt,
            );
          }

          if (!res.finished) {
            sendSSE(res, 'error', {
              type: classified.type,
              message: classified.userMessage,
              originalMessage: classified.originalMessage || scrapeError.message,
              retryable: classified.retryable,
              attemptsMade: attempt + 1,
            });
            res.end();
          }
          return;
        }

        // Otherwise, we'll retry (continue loop)
        logger.info({
          attempt,
          remainingRetries: maxRetries - attempt
        }, '[Scrape Stream] Will retry after delay...');
      }
    }

    // At this point, result should be successful
    if (!result) {
      throw new Error('[Scrape Stream] Unexpected error: result is undefined after retry loop');
    }

    try {
      // --- SAVING LOGIC ---
      sendSSE(res, 'progress', {
        step: 'saving',
        message: 'Saving transactions...',
        percent: 80,
        phase: 'saving',
        success: null
      });

      const categorizationRules = await loadCategorizationRules(client);
      const categoryMappings = await loadCategoryMappings(client);
      const billingCycleStartDay = await getBillingCycleStartDay(client);

      const stats = await processScrapedAccounts({
        client,
        accounts: result.accounts,
        companyId: options.companyId,
        credentialId,
        categorizationRules,
        categoryMappings,
        billingCycleStartDay,
        updateCategoryOnRescrape,
        isBank,
        onAccountStarted: () => true,
        onTransactionProcessed: () => true
      });

      if (!clientDisconnected) {
        sendSSE(res, 'progress', {
          step: 'endScraping',
          message: '✓ All transactions saved successfully',
          percent: 90,
          phase: 'saving',
          success: true
        });
      }
      Object.assign(accumulatedStats, stats);

    } catch (scrapeError) {
      if (auditId) {
        await updateScrapeAudit(client, auditId, 'failed', `Failed: ${scrapeError.message}`);
      }

      if (!res.finished) {
        sendSSE(res, 'error', {
          message: `Scrape Failed: ${scrapeError.message}`,
          hint: 'Please try again later or check your credentials.'
        });
        res.end();
      }
      return;
    }

    // Final Success Report
    const summary = {
      ...accumulatedStats,
      durationSeconds: Math.floor((Date.now() - startTime) / 1000)
    };

    await updateScrapeAudit(client, auditId, 'success', `Success: fetched=${accumulatedStats.transactions}, saved=${accumulatedStats.savedTransactions}`, summary, attempt, summary.durationSeconds);
    await updateCredentialLastSynced(client, credentialId);

    sendSSE(res, 'complete', {
      message: '✓ Scraping completed successfully!',
      percent: 100,
      summary: summary
    });

  } catch (error) {
    if (error instanceof VaultLockedError) {
      sendSSE(res, 'error', {
        message: error.message,
        type: 'VAULT_LOCKED'
      });
      res.end();
      return;
    }
    logger.error({ error: error instanceof Error ? error.message : String(error) }, 'Scrape stream outer catch');
    if (!res.finished) {
      sendSSE(res, 'error', { message: error instanceof Error ? error.message : 'Unknown error' });
      res.end();
    }
  } finally {
    if (client) client.release();
    if (!res.finished) res.end();
  }
}

export const config = {
  api: {
    bodyParser: true,
    responseLimit: false,
  },
};

export default handler;
