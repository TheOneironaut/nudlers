import { describe, it, expect, vi } from 'vitest';
import { BaseScraperWithBrowser } from 'israeli-bank-scrapers/lib/scrapers/base-scraper-with-browser';
import puppeteer from 'puppeteer';
import { createScraper } from 'israeli-bank-scrapers';
import { runScraper, stopAllScrapers } from '../pages/api/utils/scraperUtils';

vi.mock('israeli-bank-scrapers', () => ({ createScraper: vi.fn() }));
vi.mock('../utils/logger.js', () => ({ default: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() } }));
vi.mock('../scrapers/core.js', () => ({
    getPreparePage: () => async () => {}, getChromePath: vi.fn(), getScraperOptions: vi.fn(),
    clearActiveSession: vi.fn(), RATE_LIMITED_VENDORS: [], sleep: vi.fn(),
}));

// Explicit opt-in: these use real Chrome but never visit a provider or use credentials.
describe.skipIf(!process.env.SCRAPER_TEST_CHROME)('real browser lifecycle (offline fixtures)', () => {
    const launch = { executablePath: process.env.SCRAPER_TEST_CHROME, args: ['--no-sandbox', '--disable-dev-shm-usage'] };
    it.each(['success', 'failure', 'throw', 'timeout', 'cancel'])('closes the owned browser on %s, retaining an unrelated browser', async outcome => {
        const unrelated = await puppeteer.launch(launch);
        let browser: any;
        let ready: () => void;
        const initialized = new Promise<void>(resolve => { ready = resolve; });
        class FixtureScraper extends BaseScraperWithBrowser {
            async login() {
                ready();
                if (outcome === 'throw') throw new Error('fixture failure');
                if (outcome === 'timeout' || outcome === 'cancel') {
                    return new Promise(resolve => browser.once('disconnected', () => resolve({ success: false })));
                }
                return { success: outcome === 'success' };
            }
            async fetchData() { return { success: true, accounts: [] }; }
        }
        vi.mocked(createScraper).mockImplementation((options: any) => new FixtureScraper(options) as any);
        const client = { query: vi.fn().mockResolvedValue({ rows: [], rowCount: 1 }) };
        try {
            const run = runScraper(client, {
                ...launch, companyId: 'isracard', startDate: new Date(), timeout: outcome === 'timeout' ? 2000 : 10000,
                prepareBrowser: async (value: any) => { browser = value; },
            }, {}, null);
            const observed = run.then(result => ({ result }), error => ({ error }));
            await initialized;
            if (outcome === 'cancel') await stopAllScrapers(client);
            const result: any = await observed;
            if (outcome === 'timeout') expect(result.error.message).toContain('timed out');
            else if (outcome === 'cancel') expect(result.error.code).toBe('SCRAPE_CANCELLED');
            else expect(result.result.success).toBe(outcome === 'success');
            expect(browser.connected).toBe(false);
            expect(unrelated.connected).toBe(true);
            expect(await (await unrelated.newPage()).evaluate(() => 2 + 2)).toBe(4);
        } finally {
            await browser?.close();
            await unrelated.close();
        }
    });
});
