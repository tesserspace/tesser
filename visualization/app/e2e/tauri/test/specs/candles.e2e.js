describe("Candles", () => {
  it("creates a synthetic dataset and loads candles", async () => {
    await browser.setWindowSize(1280, 720);

    const candlesTab = await $('[data-testid="tab-candles"]');
    await candlesTab.waitForDisplayed({ timeout: 60_000 });
    await candlesTab.click();

    const createBtn = await $('[data-testid="candles-create-synthetic"]');
    await createBtn.waitForEnabled({ timeout: 60_000 });
    await createBtn.click();

    const datasetSelect = await $('[data-testid="candles-dataset-select"]');
    await datasetSelect.waitForDisplayed({ timeout: 60_000 });
    await browser.waitUntil(async () => {
      const value = await datasetSelect.getValue();
      return value === "crypto.synthetic.spot.demo.series.1s.v1";
    }, { timeout: 60_000, timeoutMsg: "dataset_id did not become synthetic dataset" });

    const loadBtn = await $('[data-testid="candles-load"]');
    await loadBtn.waitForEnabled({ timeout: 60_000 });
    await loadBtn.click();

    const meta = await $('[data-testid="candles-meta"]');
    await meta.waitForDisplayed({ timeout: 60_000 });

    await browser.waitUntil(async () => {
      const text = await meta.getText();
      if (!text || text.trim() === "—") return false;
      try {
        const parsed = JSON.parse(text);
        return typeof parsed.points_returned === "number" && parsed.points_returned > 0;
      } catch {
        return false;
      }
    }, { timeout: 120_000, timeoutMsg: "meta did not populate" });

    const canvas = await $('[data-testid="candles-canvas"]');
    await canvas.waitForDisplayed({ timeout: 60_000 });
  });
});
