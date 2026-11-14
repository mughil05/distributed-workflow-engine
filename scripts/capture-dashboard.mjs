import { mkdir } from 'node:fs/promises';
import { chromium } from '../dashboard/node_modules/playwright/index.mjs';

const base = process.env.WORKFLOW_DEMO_API_URL ?? 'http://127.0.0.1:8000';
const token = process.env.WORKFLOW_API_TOKEN;
const workflowId = process.env.WORKFLOW_DEMO_WORKFLOW_ID;
if (!token || !workflowId) throw new Error('Load .env.demo and provide WORKFLOW_DEMO_WORKFLOW_ID.');
await mkdir('docs/screenshots', { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1050 }, deviceScaleFactor: 1 });
  await page.goto(`${base}/dashboard/`, { waitUntil: 'networkidle' });
  await page.getByLabel('API token').fill(token);
  await page.getByRole('button', { name: 'Connect' }).click();
  await page.getByText(workflowId).waitFor({ timeout: 15000 });
  await page.screenshot({ path: 'docs/screenshots/dashboard-overview.png', fullPage: true });
  await page.getByText(workflowId).click();
  await page.getByText('Dependency graph').waitFor({ timeout: 15000 });
  await page.locator('.react-flow__node').filter({ hasText: 'permanent-failure' }).click();
  await page.getByText('Durable attempt history').waitFor({ timeout: 15000 });
  await page.screenshot({ path: 'docs/screenshots/dashboard-workflow-detail.png', fullPage: true });
} finally {
  await browser.close();
}
