"""
Capture one screenshot per dashboard tab into docs/screenshots/.

Prerequisites (optional dev tooling, not needed to run the dashboard):
    pip install playwright
    # uses the installed Google Chrome; or run `playwright install chromium`
    # and pass --bundled-browser

Usage:
    streamlit run dashboard/app.py          # in another terminal
    python dashboard/capture_screenshots.py [--url http://localhost:8501]

The images show whatever data source the running app uses; the source is
printed in the dashboard caption, so screenshots are self-describing.
"""

import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright

TABS = ["Revenue trend", "Top products", "Countries", "Customers", "Data quality"]
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "docs" / "screenshots"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--url", default="http://localhost:8501")
    parser.add_argument("--bundled-browser", action="store_true", help="Use Playwright's Chromium instead of Chrome")
    args = parser.parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel=None if args.bundled_browser else "chrome")
        page = browser.new_page(viewport={"width": 1440, "height": 1100}, color_scheme="light")
        page.goto(args.url, wait_until="networkidle")
        # Wait until the KPI row has rendered (first load can take ~10 s).
        page.get_by_text("Avg order value").wait_for(timeout=120_000)
        for number, tab in enumerate(TABS, start=1):
            page.get_by_role("tab", name=tab).click()
            page.wait_for_timeout(3_000)  # let Plotly finish drawing
            path = OUTPUT_DIR / f"dashboard_{number}_{tab.lower().replace(' ', '_')}.png"
            page.screenshot(path=str(path), full_page=True)
            print(f"Saved {path}")
        browser.close()


if __name__ == "__main__":
    main()
