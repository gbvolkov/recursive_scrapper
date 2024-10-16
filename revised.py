import asyncio
import os
import re
import aiofiles
import hashlib  # Для хеширования
from urllib.parse import urljoin, urlparse
from pathlib import Path
from uuid import uuid4

from playwright.async_api import async_playwright
#from markdownify import markdownify as md
from bs4 import BeautifulSoup, NavigableString
import html2text

import logging

from utils.retriever import IHTMLRetriever, IWebCrawler

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), logging.FileHandler('./output/kb_retriever.log')
    ]
)

base_url = "https://kb.ileasing.ru"
articles_url = "https://kb.ileasing.ru/space/"
global_id = "a100dc8d-3af0-418c-8634-f09f1fdb06f2"  # Replace with actual global ID
root_article = "af494df7-9560-4cb8-96d4-5b577dd4422e"

class KBHTMLRetriever(IHTMLRetriever):
    async def login(self):
        if not self.login_url:
            return  # Вход не требуется
        try:
            await self.page.goto(self.login_url)
            await self.wait_for_page_load(self.page)
            await self.page.get_by_role("button").first.click()
            await self.page.get_by_placeholder("Введите логин").fill(self.login_credentials['username'])
            await self.page.get_by_placeholder("Введите пароль").click()
            await self.page.get_by_placeholder("Введите пароль").fill(self.login_credentials['password'])
            await self.page.get_by_role("button", name="войти").click()
            await self.page.wait_for_timeout(2000)
            logging.info(f"Успешный вход на {self.login_url}")
            return True
        except Exception as e:
            logging.error(f"Не удалось выполнить вход на {self.login_url}: {e}")
            return False

    async def clean_content(self, html_content):
        html_content = await super().clean_content(html_content)
        soup = BeautifulSoup(html_content, 'html.parser')
        for element in soup.find_all('div', class_='article-info editor__article-info'):
            element.decompose()
        for element in soup.find_all('div', class_='article-properties editor__properties'):
            element.decompose()
        if content := soup.find(
            'div', class_='editor__body-content editor-container'
        ):
            return str(content)
        logging.error(f"Не удалось получить контент статьи для {self.page.url}")
        if self.page.url.startswith(articles_url):
            return None
        return html_content


class KBWebCrawler(IWebCrawler):
    async def get_links(self, soup, url):
        std_links = await super().get_links(soup, url)
        links = []
        if nested_content := soup.find(
            'div', class_=['scrollbar', 'nested-articles__content', 'ps']
        ):
            li_elements = nested_content.find_all('li')
            for li in li_elements:
                keyname = li.get('keyname')
                ancestorids = li.get('ancestorids')
                if keyname and ancestorids:
                    link_url = f"{base_url}/space/{global_id}/article/{keyname}"
                    links.append((li, link_url))

        std_links.extend(links)
        return std_links

    def get_title(self, soup, url):
        if title := soup.find(
            'p', class_=['editor-title__text']
        ):
            return title.text
        return super().get_title(soup, url)


from dotenv import load_dotenv,dotenv_values
import os
from pathlib import Path
documents_path = Path.home() / ".env"
load_dotenv(os.path.join(documents_path, 'gv.env'))
USERNAME = '7810155'
PASSWORD = os.environ.get('IL_PWD')

async def main():
    # Задайте ваш стартовый URL
    start_url = "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/af494df7-9560-4cb8-96d4-5b577dd4422e"
    #start_url = "https://quotes.toscrape.com/page/1/"
    login_url = f"{base_url}/auth/sign-in?redirect=%2Fspace%2F{global_id}%2Farticle%2F{root_article}"
    #login_url = "https://example.com/login"  # Замените на ваш URL для входа, если необходимо
    login_credentials = {
        "username": USERNAME,
        "password": PASSWORD
    }

    # Инициализация retriever без логина
    async with KBHTMLRetriever(base_url=start_url, login_url=login_url, login_credentials=login_credentials) as retriever:
        # Если требуется логин, раскомментируйте следующие строки:
        if not await retriever.login():
            crawler = KBWebCrawler(
                retriever,
                #duplicate_tags=['div', 'p', 'table'],
                #duplicate_tags=[],
                no_images=False,
                max_depth=8,
                non_recursive_classes=['tag'],
                #navigation_classes=['side_categories', 'pager'],  # Ваши навигационные классы
                #ignored_classes = ['footer', 'row header-box', 'breadcrumb', 'header container-fluid', 'icon-star', 'image_container']
            )
            allowed_domains = ['kb.ileasing.ru', ""]
            start_urls = [
                #FAQ
                'https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/e7a19a56-d067-4023-b259-94284ec4e16b',
                'https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/a1038bbc-e5d9-4b5a-9482-2739c19cb6cb',
                'https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/3fdb4f97-2246-4b9e-b477-e9d7d8a2eb86',
                'https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/dd64ab73-50ea-4d48-83f0-8dcef88512cb',
                
                # Инструкции ОИТ
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/af494df7-9560-4cb8-96d4-5b577dd4422e",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/508e24c5-aa23-419d-9251-69a2bf096706",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/bb0c7555-f7b3-48a0-9fa1-f3708842ca1a",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/0ccc2abb-b7cd-44c5-bddb-91e055e545cd",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/26df2ad9-29b3-4ec9-82b4-fd21fcd14dec",
                # Пользовательские инструкции
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/602810e3-eb3c-47b8-bbfe-44be5c33566b",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/4f81f5fe-cd15-492f-8aa0-66b3e4313a85",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/7c72943d-3f2d-41f9-a1ec-db027880d615",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/8e30fae5-f94f-4efd-a633-997a19cd891c",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/3b04ba0f-e24d-4ff6-ba59-60b869b67b16",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/916983d3-f0e5-48f0-a1ab-4ec104035963",
                "https://kb.ileasing.ru/space/a100dc8d-3af0-418c-8634-f09f1fdb06f2/article/3edc1530-3fbe-4a9e-8ea2-6876a2a63683"
            ]
            for start_url in start_urls:
                crawler.initialize() 
                await crawler.crawl(start_url)

if __name__ == "__main__":
    asyncio.run(main())
