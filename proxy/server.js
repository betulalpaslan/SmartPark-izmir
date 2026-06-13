const http = require('http');
const puppeteer = require('puppeteer-core');

const CHROME_PATH = 'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe';
const API_URL = 'https://openapi.izmir.bel.tr/api/ibb/izum/otoparklar';
const PORT = 9999;
const REFRESH_MS = 30_000;

let cachedData = null;
let browser = null;

async function getBrowser() {
  if (!browser || !browser.connected) {
    browser = await puppeteer.launch({
      executablePath: CHROME_PATH,
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu'],
    });
  }
  return browser;
}

async function fetchLoop() {
  while (true) {
    let page = null;
    try {
      const b = await getBrowser();
      page = await b.newPage();
      await page.goto('about:blank');
      const text = await page.evaluate(async (url) => {
        const r = await fetch(url, {
          headers: { 'Accept': 'application/json', 'Accept-Language': 'tr-TR,tr;q=0.9' }
        });
        return r.text();
      }, API_URL);
      const data = JSON.parse(text);
      cachedData = JSON.stringify(data);
      console.log(`[${new Date().toISOString()}] Guncellendi — ${data.length} otopark`);
    } catch (e) {
      console.error(`[${new Date().toISOString()}] Hata: ${e.message}`);
    } finally {
      if (page) await page.close().catch(() => {});
    }
    await new Promise(r => setTimeout(r, REFRESH_MS));
  }
}

const server = http.createServer((req, res) => {
  if (req.url !== '/otoparklar') {
    res.writeHead(404); res.end('Not found'); return;
  }
  if (!cachedData) {
    res.writeHead(503); res.end(JSON.stringify({ error: 'Veri henuz hazir degil' })); return;
  }
  res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(cachedData);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Proxy http://0.0.0.0:${PORT}/otoparklar adresinde dinliyor`);
  fetchLoop();
});
