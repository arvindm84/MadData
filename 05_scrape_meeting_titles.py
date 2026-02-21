"""
Step 1: Scrape Meeting Titles from Madison City Channel
=======================================================
HTML structure per meeting (confirmed from live page):

    <div>TITLE + DATE STRING</div>
    <div>description (often empty)</div>
    <div>City Staff</div>
    <div>upload timestamp</div>
    <div><a href="...Presentation/XXX/Info">View</a></div>
    <hr/>

So the title div is exactly 4 previous siblings before the <a> tag.

Install deps:
    pip install requests beautifulsoup4
"""

import re
import json
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup

CHANNELS = [
    {
        "name": "neighborhood_meetings",
        "url":  "https://media.cityofmadison.com/Mediasite/Showcase/madison-city-channel/Channel/neighborhood_meeting/Info",
    },
    {
        "name": "plan_commission",
        "url":  "https://media.cityofmadison.com/Mediasite/Showcase/madison-city-channel/channel/plan-commission/Info",
    },
    {
        "name": "additional_meetings",
        "url":  "https://media.cityofmadison.com/Mediasite/Showcase/madison-city-channel/Channel/additional-meetings/Info",
    },
]

OUTPUT_JSON = Path("all_meetings.json")


def parse_channel(channel_name, url):
    print(f"\nFetching: {channel_name}")
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [!] Failed: {e}")
        return []

    if "Javascript is required" in resp.text:
        print(f"  [!] Page requires JavaScript — Selenium needed for this channel")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find every <a> tag pointing to a Presentation URL
    links = soup.find_all("a", href=re.compile(r"/Presentation/[a-f0-9]+1d", re.I))
    print(f"  Found {len(links)} presentation links")

    meetings = []
    for link in links:
        href = link["href"]
        if not href.endswith("/Info"):
            href = href.rstrip("/") + "/Info"

        # Walk back through previous siblings (all are <div> tags)
        # Structure: title_div, desc_div, citystaff_div, upload_date_div, [this link's div]
        prev_divs = []
        for sib in link.parent.find_previous_siblings("div"):
            prev_divs.append(sib.get_text(strip=True))
            if len(prev_divs) == 4:
                break

        # prev_divs[0] = upload timestamp
        # prev_divs[1] = "City Staff"
        # prev_divs[2] = description (may be empty)
        # prev_divs[3] = title + meeting date string

        raw_title = prev_divs[3] if len(prev_divs) >= 4 else ""
        description = prev_divs[2] if len(prev_divs) >= 3 and prev_divs[2] not in ("", "City Staff") else None

        # The title div contains "TITLE DATE TIME" — strip the trailing date/time
        # Dates look like: 4/22/2021 6:03 PM  or  5/13/2021 6:35 PM
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s+\d{1,2}:\d{2}', raw_title)
        date  = date_match.group(1) if date_match else None
        title = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{4}.*$', '', raw_title).strip()

        meetings.append({
            "channel":          channel_name,
            "title":            title or raw_title,
            "date":             date,
            "description":      description,
            "presentation_url": href,
            "relevant":         None,
        })

    return meetings


def scrape_all():
    all_meetings = []
    for ch in CHANNELS:
        meetings = parse_channel(ch["name"], ch["url"])
        all_meetings.extend(meetings)
        time.sleep(1)

    OUTPUT_JSON.write_text(json.dumps(all_meetings, indent=2))

    print(f"\n{'='*55}")
    print(f"Total meetings scraped: {len(all_meetings)}")
    print(f"Saved → {OUTPUT_JSON}")
    print(f"\nSample titles:")
    for m in all_meetings[:8]:
        print(f"  [{m['date']}] {m['title']}")

    return all_meetings


if __name__ == "__main__":
    scrape_all()