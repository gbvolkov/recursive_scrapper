import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from playwright.async_api import (
    Browser,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)


PROMPT_TEMPLATE = """
You are assisting a sourcing specialist who reviews LinkedIn recruiter profiles.
Summarise the profile and evaluate whether the recruiter appears to be actively working.

Respond strictly in valid JSON with the following keys:
{
  "profile_summary": string,  // 3-4 sentences, highlight seniority, focus areas, industries, achievements.
  "activity_assessment": string,  // Explain cues of recent activity (posts, current role dates, keywords).
  "is_active_recruiter": boolean  // true if evidence suggests they are actively recruiting for roles now.
}

Profile content:
{profile_text}
""".strip()


@dataclass
class ProfileResult:
    name: str
    linkedin_url: str
    profile_summary: str
    activity_assessment: str
    profile_status: str
    is_active_recruiter: Optional[bool] = None


class LinkedInSession:
    def __init__(self, email: str, password: str, headless: bool = False) -> None:
        self.email = email
        self.password = password
        self.headless = headless
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None

    async def __aenter__(self) -> "LinkedInSession":
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        context = await self._browser.new_context()
        self._page = await context.new_page()
        await self._login()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    @property
    def page(self) -> Page:
        if not self._page:
            raise RuntimeError("Playwright page not initialised.")
        return self._page

    async def _login(self) -> None:
        page = self.page
        await page.goto("https://www.linkedin.com/login", wait_until="networkidle")

        if await self._is_authenticated():
            return

        username_locator = page.locator("input#username")
        password_locator = page.locator("input#password")

        if await username_locator.count() and await password_locator.count():
            await username_locator.fill(self.email)
            await password_locator.fill(self.password)
            await page.click("button[type='submit']")
        else:
            # Login form not present; wait to detect existing authenticated session.
            await page.wait_for_load_state("networkidle")

        await self._wait_for_authentication()

    async def _wait_for_authentication(self, timeout_seconds: float = 90.0) -> None:
        page = self.page
        deadline = asyncio.get_running_loop().time() + timeout_seconds

        while True:
            if await self._is_authenticated():
                return

            current_url = page.url
            if "checkpoint" in current_url or "challenge" in current_url:
                raise RuntimeError(
                    "LinkedIn is asking for additional verification (checkpoint/challenge page). "
                    "Complete the verification manually and rerun."
                )

            if asyncio.get_running_loop().time() > deadline:
                raise RuntimeError(
                    "LinkedIn login not confirmed within allotted time. "
                    "If you completed 2FA manually, rerun the script."
                )

            await page.wait_for_timeout(1500)

    async def _is_authenticated(self) -> bool:
        page = self.page
        current_url = page.url or ""

        if "login" in current_url:
            return False
        if "checkpoint" in current_url or "challenge" in current_url:
            return False

        selectors = [
            "input#global-nav-typeahead",
            "input[placeholder*='Search'][role='combobox']",
            "input[placeholder='Search'][aria-label*='Search']",
            "button[aria-label*='My LinkedIn']",
            "button[aria-label*='profile']",
            "header.global-nav",
        ]

        for selector in selectors:
            locator = page.locator(selector)
            try:
                if await locator.count():
                    return True
            except PlaywrightTimeoutError:
                continue

        try:
            has_nav = await page.evaluate(
                "Boolean(document.querySelector('header[class*=\"global-nav\"]') || "
                "document.querySelector('a[href*=\"/messaging/\"]') || "
                "document.querySelector('button[aria-label*=\"Start a post\"]'))"
            )
        except Exception:
            has_nav = False

        if has_nav:
            return True

        return current_url.startswith("https://www.linkedin.com/feed/")

    async def fetch_profile_text(self, url: str) -> Dict[str, Any]:
        page = self.page
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
        except PlaywrightTimeoutError:
            return {"status": "NOK", "message": "Timeout while loading profile.", "text": ""}

        await page.wait_for_timeout(1500)
        html_segments: List[str] = []

        for selector in [
            "main",
            "section.pv-profile-section",
            "section[data-view-name='profile_info']",
        ]:
            locator = page.locator(selector)
            if await locator.count():
                html_segments.append(await locator.first.inner_html())

        if not html_segments:
            body_text = await page.inner_text("body")
            lowered = body_text.lower()
            if "profile unavailable" in lowered or "profile not found" in lowered:
                return {"status": "NOK", "message": "Profile unavailable.", "text": ""}
            return {"status": "OK", "text": body_text}

        soup = BeautifulSoup(" ".join(html_segments), "html.parser")
        text = " ".join(chunk.strip() for chunk in soup.stripped_strings if chunk.strip())
        if not text:
            return {"status": "NOK", "message": "Unable to extract profile text.", "text": ""}
        return {"status": "OK", "text": text}


class RecruiterProfiler:
    def __init__(self, llm: ChatOpenAI) -> None:
        self.llm = llm
        self.prompt = PromptTemplate.from_template(PROMPT_TEMPLATE)

    def analyse(self, profile_text: str) -> Dict[str, Any]:
        if not profile_text:
            return {
                "profile_summary": "",
                "activity_assessment": "Profile data missing.",
                "is_active_recruiter": None,
            }

        prompt_text = self.prompt.format(profile_text=profile_text[:10000])
        response = self.llm.invoke(prompt_text)
        content = getattr(response, "content", response)
        if not isinstance(content, str):
            content = json.dumps(content)
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return {
                "profile_summary": content,
                "activity_assessment": "LLM response not valid JSON.",
                "is_active_recruiter": None,
            }

        return {
            "profile_summary": parsed.get("profile_summary", ""),
            "activity_assessment": parsed.get("activity_assessment", ""),
            "is_active_recruiter": parsed.get("is_active_recruiter"),
        }


async def process_profiles(
    excel_path: Path,
    output_path: Path,
    linkedin_email: str,
    linkedin_password: str,
    openai_model: str = "gpt-4o-mini",
    headless: bool = False,
) -> List[ProfileResult]:
    df = pd.read_excel(excel_path)
    if "Linkedin" not in df.columns:
        raise ValueError("Expected column 'Linkedin' not found in the spreadsheet.")

    llm = ChatOpenAI(model=openai_model, temperature=0.1)
    profiler = RecruiterProfiler(llm=llm)

    results: List[ProfileResult] = []
    async with LinkedInSession(email=linkedin_email, password=linkedin_password, headless=headless) as session:
        for _, row in df.iterrows():
            first_name = str(row.get("Name", "")).strip()
            last_name = str(row.get("Family Name", "")).strip()
            full_name = " ".join(part for part in [first_name, last_name] if part)
            linkedin_url = str(row.get("Linkedin", "")).strip()

            if not linkedin_url:
                results.append(
                    ProfileResult(
                        name=full_name or "Unknown",
                        linkedin_url="",
                        profile_summary="",
                        activity_assessment="Missing LinkedIn URL.",
                        profile_status="NOK",
                    )
                )
                continue

            profile_payload = await session.fetch_profile_text(linkedin_url)
            analysis = profiler.analyse(profile_payload.get("text", ""))

            results.append(
                ProfileResult(
                    name=full_name or "Unknown",
                    linkedin_url=linkedin_url,
                    profile_summary=analysis["profile_summary"],
                    activity_assessment=analysis["activity_assessment"],
                    profile_status=profile_payload.get("status", "NOK"),
                    is_active_recruiter=analysis.get("is_active_recruiter"),
                )
            )

    output_records: List[Dict[str, Any]] = []
    for item in results:
        if item.is_active_recruiter is True:
            activity_flag = "ACTIVE"
        elif item.is_active_recruiter is False:
            activity_flag = "INACTIVE"
        else:
            activity_flag = "UNKNOWN"

        output_records.append(
            {
                "name": item.name,
                "linkedin_url": item.linkedin_url,
                "profile_summary": item.profile_summary,
                "activity_assessment": item.activity_assessment,
                "activity_flag": activity_flag,
                "profile_status": item.profile_status,
            }
        )

    output_df = pd.DataFrame(output_records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_excel(output_path, index=False)
    return results


def main() -> None:
    excel_file = Path(os.environ.get("RECRUITER_EXCEL", "Copy of Hunters_for_MailTrack_final(1).xlsx"))
    output_file = Path(os.environ.get("RECRUITER_OUTPUT", "output/linkedin_profile_summary.xlsx"))

    linkedin_email = os.environ.get("LINKEDIN_EMAIL")
    linkedin_password = os.environ.get("LINKEDIN_PASSWORD")
    if not linkedin_email or not linkedin_password:
        raise RuntimeError("Environment variables LINKEDIN_EMAIL and LINKEDIN_PASSWORD must be set.")

    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY must be set to call the LLM.")

    headless = os.environ.get("PLAYWRIGHT_HEADLESS", "0") not in {"0", "false", "False"}

    asyncio.run(
        process_profiles(
            excel_path=excel_file,
            output_path=output_file,
            linkedin_email=linkedin_email,
            linkedin_password=linkedin_password,
            headless=headless,
        )
    )


if __name__ == "__main__":
    main()
