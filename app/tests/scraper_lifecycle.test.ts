import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createScraper } from 'israeli-bank-scrapers';
import { runScraper, stopAllScrapers, resetCategoryCache } from '../pages/api/utils/scraperUtils';
import { createScraperLifecycle } from '../scrapers/lifecycle.js';

vi.mock('israeli-bank-scrapers', () => ({ createScraper: vi.fn() }));
vi.mock('../utils/logger.js', () => ({ default: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() } }));
vi.mock('../scrapers/core.js', () => ({
    getPreparePage: vi.fn(), getChromePath: vi.fn(), getScraperOptions: vi.fn(),
    clearActiveSession: vi.fn(), RATE_LIMITED_VENDORS: [], sleep: vi.fn(),
}));

const client = { query: vi.fn().mockResolvedValue({ rows: [], rowCount: 1 }) };
const options = { companyId: 'isracard', startDate: new Date(), timeout: 1000 };
let scraper: any;
let terminate: any;
const never = () => new Promise(() => {});

beforeEach(() => {
    vi.useFakeTimers();
    resetCategoryCache();
    client.query.mockReset().mockResolvedValue({ rows: [], rowCount: 1 });
    terminate = vi.fn().mockResolvedValue(undefined);
    scraper = { options: {}, terminate, scrape: vi.fn(), onProgress: vi.fn() };
    vi.mocked(createScraper).mockReturnValue(scraper);
});
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

describe('runScraper resource ownership', () => {
    it.each([
        { success: true, accounts: [] },
        { success: true },
        { success: false, errorType: 'INVALID_PASSWORD' },
        { success: true, accounts: [{ txns: [] }] },
    ])('closes deferred browsers for result %j', async result => {
        scraper.scrape.mockImplementation(async () => { await scraper.terminate(); return result; });
        expect(await runScraper(client, options, {}, null)).toEqual(result);
        expect(terminate).toHaveBeenCalledTimes(1);
        expect(vi.getTimerCount()).toBe(0);
    });

    it.each(['isracard', 'leumi', 'hapoalim'])('closes %s on an initialization exception', async companyId => {
        const error = new Error('initialization failed');
        scraper.scrape.mockRejectedValue(error);
        await expect(runScraper(client, { ...options, companyId }, {}, null)).rejects.toBe(error);
        expect(terminate).toHaveBeenCalledTimes(1);
    });

    it('retains the browser through category fetching, then closes it', async () => {
        scraper.page = { isClosed: () => false, evaluate: vi.fn(async () => {
            expect(terminate).not.toHaveBeenCalled();
            return {};
        }) };
        scraper.scrape.mockResolvedValue({ success: true, accounts: [{ txns: [{ identifier: 'fixture', description: 'Fixture', date: '2026-01-01' }] }] });
        await runScraper(client, options, {}, null);
        expect(scraper.page.evaluate).toHaveBeenCalled();
        expect(terminate).toHaveBeenCalledTimes(1);
    });

    it.each(['isracard', 'leumi'])('cleans a timed-out %s scrape', async companyId => {
        scraper.scrape.mockImplementation(never);
        const run = runScraper(client, { ...options, companyId }, {}, null);
        const rejected = expect(run).rejects.toThrow('timed out');
        await vi.advanceTimersByTimeAsync(1000);
        await rejected;
        expect(terminate).toHaveBeenCalledTimes(1);
    });

    it('keeps the global timeout active during category queries', async () => {
        client.query.mockResolvedValueOnce({ rows: [] }).mockImplementation(never);
        scraper.scrape.mockResolvedValue({ success: true, accounts: [{ txns: [] }] });
        const rejected = expect(runScraper(client, options, {}, null)).rejects.toThrow('timed out');
        await vi.advanceTimersByTimeAsync(1000);
        await rejected;
        expect(terminate).toHaveBeenCalledTimes(1);
    });

    it('cancels an active operation and closes only its browser', async () => {
        scraper.scrape.mockImplementation(never);
        const unrelated = { close: vi.fn() };
        const rejected = expect(runScraper(client, options, {}, null)).rejects.toMatchObject({ code: 'SCRAPE_CANCELLED' });
        await vi.advanceTimersByTimeAsync(0);
        await stopAllScrapers(client);
        await rejected;
        expect(terminate).toHaveBeenCalledTimes(1);
        expect(unrelated.close).not.toHaveBeenCalled();
        expect(vi.getTimerCount()).toBe(0);
    });

    it('honors cancellation while the base scrape is pending', async () => {
        let cancel = false;
        scraper.scrape.mockImplementation(never);
        const rejected = expect(runScraper(client, options, {}, null, () => cancel)).rejects.toMatchObject({ code: 'SCRAPE_CANCELLED' });
        await vi.advanceTimersByTimeAsync(0);
        cancel = true;
        await vi.advanceTimersByTimeAsync(250);
        await rejected;
        expect(terminate).toHaveBeenCalledTimes(1);
    });

    it('preserves the scrape error when termination fails', async () => {
        const error = new Error('original failure');
        scraper.scrape.mockRejectedValue(error);
        terminate.mockRejectedValue(new Error('cleanup failure'));
        await expect(runScraper(client, options, {}, null)).rejects.toBe(error);
        expect(vi.getTimerCount()).toBe(0);
    });
});

describe('late launches and bounded cleanup', () => {
    it('closes a browser launched after finalization before page initialization', async () => {
        const lifecycle = createScraperLifecycle(scraper, { deferTermination: true });
        await lifecycle.finish();
        const browser = { close: vi.fn().mockResolvedValue(undefined), process: () => null };
        await expect(scraper.options.prepareBrowser(browser)).rejects.toMatchObject({ code: 'SCRAPE_CANCELLED' });
        expect(browser.close).toHaveBeenCalledTimes(1);
    });

    it('kills only the owned child if graceful termination hangs', async () => {
        terminate.mockImplementation(never);
        const lifecycle = createScraperLifecycle(scraper);
        const child = { exitCode: null, signalCode: null, kill: vi.fn() };
        await scraper.options.prepareBrowser({ process: () => child });
        const finish = lifecycle.finish();
        await vi.advanceTimersByTimeAsync(5000);
        await finish;
        expect(child.kill).toHaveBeenCalledWith('SIGKILL');
    });
});
