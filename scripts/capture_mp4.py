"""
Capture an MP4 of the Oct 30 storm map.

Renders storm time 14:30–17:00 ET into a ~15-second MP4, cropped to the
map canvas only (panel and timeline bar removed).
Output: /app/output/storm_oct30.mp4

Usage (via make):
    make mp4
"""

import asyncio
import subprocess
from datetime import datetime
from pathlib import Path

import pytz
from playwright.async_api import async_playwright

# ── Time range ────────────────────────────────────────────────────────
ET = pytz.timezone("America/New_York")
START_MS = int(ET.localize(datetime(2025, 10, 30, 14, 30, 0)).timestamp() * 1000)
END_MS   = int(ET.localize(datetime(2025, 10, 30, 17,  0, 0)).timestamp() * 1000)

# ── Output settings ───────────────────────────────────────────────────
FRAMES_DIR  = Path("/tmp/mp4_frames")
OUTPUT_MP4  = Path("/app/output/storm_oct30.mp4")
FPS         = 15
DURATION_S  = 15          # target length in seconds
N_FRAMES    = FPS * DURATION_S
STEP_MS     = (END_MS - START_MS) // N_FRAMES

# ── Map settings ─────────────────────────────────────────────────────
VIEWPORT    = {"width": 1200, "height": 800}
VIEW_STATE  = {"longitude": -73.976, "latitude": 40.72, "zoom": 9.2,
               "bearing": 0, "pitch": 0}


async def main() -> None:
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport=VIEWPORT)

        print("Loading map…")
        await page.goto("file:///app/output/storm_oct30.html")
        await page.wait_for_load_state("networkidle", timeout=30_000)

        # Stop animation and set zoom
        await page.evaluate(f"""() => {{
            isPlaying = false;
            deckgl.setProps({{ viewState: {VIEW_STATE} }});
        }}""")
        await asyncio.sleep(3)   # let basemap tiles settle

        print(f"Capturing {N_FRAMES} frames…")
        for i in range(N_FRAMES):
            t_ms = START_MS + i * STEP_MS
            await page.evaluate(f"""() => {{
                currentT = {t_ms};
                render(currentT);
            }}""")
            await asyncio.sleep(0.05)
            await page.screenshot(path=str(FRAMES_DIR / f"frame_{i:04d}.png"))
            if (i + 1) % 30 == 0:
                print(f"  {i + 1}/{N_FRAMES}")

        await browser.close()

    print("Assembling MP4…")
    subprocess.run([
        "ffmpeg", "-y",
        "-framerate", str(FPS),
        "-i", str(FRAMES_DIR / "frame_%04d.png"),
        "-vf", "crop=546:506:290:169",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18",
        str(OUTPUT_MP4),
    ], check=True)
    size_mb = OUTPUT_MP4.stat().st_size / 1_048_576
    print(f"✓  Written: {OUTPUT_MP4}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    asyncio.run(main())
