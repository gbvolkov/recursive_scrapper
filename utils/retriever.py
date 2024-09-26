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

USER_AGENT = "ILCrawler/1.0 (+http://gbvolkoff.name/crawler)"
INSIGNIFICANT_TAGS = ['small', 'strong', 'em', 'span', 'b', 'i', 'u', 'sup', 'sub']

def get_header(headers, key):
    """
    Получает значение заголовка независимо от регистра.
    """
    return next((value for k, value in headers.items() if k.lower() == key.lower()), '')

def prepare_tag_for_hashing(tag, insignificant_tags, base_url):
    """
    Готовит тег для хеширования.
    """
    tag_copy = tag.__copy__()

    #for sub_tag in tag_copy.find_all(insignificant_tags):
    #    sub_tag.unwrap()

    #contains_link = False
    cleaned_html = ""
    for a_tag in tag_copy.find_all('a', href=True):
        #contains_link = True
        original_href = a_tag['href']
        a_tag['href'] = urljoin(base_url, original_href)
        cleaned_html += a_tag['href']

    return cleaned_html

def replace_tag(tag, replacement_text):
    """
    Заменяет HTML-тег на заданный текст или HTML.
    """
    tag.replace_with(replacement_text)

def has_ignored_class(tag, ignored_classes):
    """
    Проверяет, содержит ли тег любой из игнорируемых классов.
    """
    tag_classes = tag.get('class', [])
    return any(cls in ignored_classes for cls in tag_classes)


class IHTMLRetriever:
    def __init__(self, base_url, login_url=None, login_credentials=None, user_agent=None):
        """
        Инициализация HTML Retriever.
        """
        self.base_url = base_url
        self.login_url = login_url
        self.login_credentials = login_credentials or {}
        self.user_agent = user_agent or USER_AGENT
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context(user_agent=self.user_agent)
        self.page = await self.context.new_page()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def wait_for_page_load(self, timeout=30000):
        try:
            # Wait for the page to reach the 'load' state
            await self.page.wait_for_load_state('load', timeout=timeout)
            
            # Wait for any remaining dynamic content
            await self.page.evaluate('''() => {
                return new Promise((resolve) => {
                    if (document.readyState === 'complete') {
                        // Add a small delay to allow for any final rendering
                        setTimeout(resolve, 1000);
                    } else {
                        window.addEventListener('load', () => setTimeout(resolve, 1000));
                    }
                })
            }''')
            
            # Optional: Check for any loading indicators
            loading_indicator_gone = await self.page.evaluate('''() => {
                const loaders = document.querySelectorAll('.loading, .spinner, .loader');
                return loaders.length === 0;
            }''')
            
            if not loading_indicator_gone:
                print(f"Warning: Possible loading indicators still present on {self.page.url}")
            
        except TimeoutError:
            print(f"Timeout waiting for page to load: {self.page.url}")
        
        # Capture any console errors
        self.page.on("console", lambda msg: print(f"Console {msg.type}: {msg.text}") if msg.type == "error" else None)

    async def login(self):
        if not self.login_url:
            return  # Вход не требуется
        try:
            raise "Not implemented"
        except Exception as e:
            logging.error(f"Не удалось выполнить вход на {self.login_url}: {e}")

    async def clean_content(self, html_content):
        return str(html_content)

    async def retrieve_content(self, url):
        """
        Получает HTML-контент по заданному URL.
        """
        try:
            response = await self.page.goto(url, timeout=30000)  # Таймаут 30 секунд
            if response is None:
                logging.warning(f"Нет ответа для {url}")
                return ""
            status = response.status
            if status >= 400:
                logging.warning(f"Получен статус {status} для {url}")
                return ""
            await self.wait_for_page_load(self.page)
            await self.page.wait_for_timeout(2000)

            content_type = get_header(response.headers, 'Content-Type').lower()
            if 'text/html' not in content_type:
                logging.warning(f"Пропуск не-HTML контента: {url}")
                return ""
            html_content = await self.page.content()

            content = await self.clean_content(html_content)

            return content
        except Exception as e:
            logging.error(f"Не удалось получить {url}: {e}")
            return ""

class IWebCrawler:
    def __init__(self, retriever, output_dir='output', images_dir='images', duplicate_tags=None, no_images=False, max_depth=5, non_recursive_classes=None, navigation_classes=None, ignored_classes=None):
        """
        Инициализация WebCrawler.
        """
        self.retriever = retriever
        self.output_dir = Path(output_dir)
        self.no_images = no_images
        self.images_dir = self.output_dir / images_dir
        self.base_netloc = urlparse(retriever.base_url).netloc
        self.max_depth = max_depth
        self.non_recursive_classes = non_recursive_classes or []
        self.ignored_classes = ignored_classes or []

        # Навигационные классы
        self.navigation_classes = navigation_classes or []

        # Список тегов для проверки дубликатов
        self.duplicate_tags = duplicate_tags or []
        self.initialize()

        # Создание директорий для вывода и изображений
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)

    def initialize(self):
        self.visited = set()
        self.processed_elements = set()
        self.processed_navigation = set()

    def sanitize_filename(self, url):
        """
        Санифицирует URL для использования в качестве имени файла.
        """
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or "index"
        filename = f"{parsed.netloc}_{path}.md"
        filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
        return filename

    async def save_markdown(self, filename, content, title=None, url=None):
        """
        Сохраняет Markdown-контент в файл с YAML фронтматером.
        """
        if content is None:
            content = ""
        file_path = self.output_dir / filename
        front_matter = f"---\nTITLE: \"{title or filename}\"\nurl: \"{url or ''}\"\n---\n\n" if title or url else ""
        async with aiofiles.open(file_path, 'a', encoding='utf-8') as f:
            await f.write(front_matter + content + "\n\n===================================\n\n")

    async def save_image(self, img_url, retries=3, delay=2):
        """
        Скачивает и сохраняет изображение с механизом повторных попыток.
        """
        for attempt in range(1, retries + 1):
            try:
                if img_url.startswith('data:'):
                    logging.warning(f"Пропуск изображения с data URI: {img_url}")
                    return ""
                response = await self.retriever.page.goto(img_url, timeout=10000)  # Таймаут 10 секунд
                if response is None:
                    logging.warning(f"Не удалось скачать изображение: {img_url}")
                    return ""
                content_type = get_header(response.headers, 'Content-Type').lower()
                if all(
                    ct not in content_type
                    for ct in [
                        'image/jpeg',
                        'image/png',
                        'image/gif',
                        'image/webp',
                    ]
                ):
                    logging.warning(f"Пропуск не-изображения: {img_url}")
                    return ""
                img_bytes = await response.body()
                parsed = urlparse(img_url)
                ext = os.path.splitext(parsed.path)[1] or '.png'
                img_content_hash = hashlib.md5(img_bytes).hexdigest()
                img_name = re.sub(r'[\\/*?:"<>|]', "_", parsed.path.strip("/").replace("/", "_")) or "image"
                img_filename = f"{img_name}_{img_content_hash}{ext}"
                img_path = self.images_dir / img_filename
                async with aiofiles.open(img_path, 'wb') as f:
                    await f.write(img_bytes)
                logging.info(f"Сохранено изображение: {img_filename}")
                return img_filename
            except Exception as e:
                logging.error(f"Попытка {attempt} - Ошибка при сохранении изображения {img_url}: {e}")
                if attempt < retries:
                    logging.info(f"Повтор через {delay} секунд...")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Не удалось сохранить изображение после {retries} попыток: {img_url}")
                    return ""

    async def process_navigation_link(self, link_url, current_depth = 0, filename = None):
        """
        Обрабатывает ссылку из навигационного элемента без увеличения глубины.
        """
        if filename is None:
            filename = self.sanitize_filename(link_url)
        if link_url not in self.visited:
            logging.info(f"Обработка навигационной ссылки: {link_url}")
            content = await self.process_page(link_url, filename=filename, current_depth=current_depth)
            markdown = self.html_to_markdown(content)
            await self.save_markdown(filename, markdown)

    async def remove_ignored_elements(self, soup, url):
        #Удаляем игнорируемые элементы
        for ignored_class in self.ignored_classes:
            ignored_elements = soup.find_all(class_=ignored_class)
            for ignored in ignored_elements:
                ignored.decompose()
                logging.debug(f"Удалён элемент с классом {ignored_class} из {url}")

    async def get_navigators(self, soup, url):
        # Обработка навигационных элементов
        navigators = []
        for nav_class in self.navigation_classes:
            nav_elements = soup.find_all(class_=nav_class)
            for nav in nav_elements:
                hashed_content = prepare_tag_for_hashing(nav, INSIGNIFICANT_TAGS, base_url=url)
                nav_hash = hashlib.md5(hashed_content.encode('utf-8')).hexdigest()
                if nav_hash not in self.processed_navigation:
                    logging.debug(f"Добавлен в очередь навигационный элемент с классом {nav_class} из {url}")
                    self.processed_navigation.add(nav_hash)
                    navigators.append(nav.__copy__())
                # Удаление навигационного элемента из содержимого страницы
                nav.decompose()
                logging.debug(f"Удалён навигационный элемент с классом {nav_class} из {url}")

        return navigators

    async def remove_duplicates(self, soup, url):
        # Обработка дублирующихся элементов
        tags_to_decompose = []
        for tag in soup.find_all(self.duplicate_tags):
            cleaned_content = prepare_tag_for_hashing(tag, INSIGNIFICANT_TAGS, base_url=url)
            if not cleaned_content:
                continue
            tag_hash = hashlib.md5(cleaned_content.encode('utf-8')).hexdigest()

            if tag_hash in self.processed_elements:
                tags_to_decompose.append(tag)
                logging.debug(f"Дублирующий элемент {tag} будет пропущен на {url}")
            else:
                self.processed_elements.add(tag_hash)
                logging.debug(f"Новый элемент обработан на {url}")

        for tag in tags_to_decompose:
            tag.decompose()
            logging.debug(f"Дублирующий элемент удалён из {url}")
        return

    async def get_images_elements(self, soup):
        return [(img, img['src']) for img in soup.find_all('img') if img.get('src')]

    async def save_images(self, soup, url):
        # Обработка изображений
        for img in await self.get_images_elements(soup):
            img_element = img[0]
            img_url = urljoin(url, img[1])
            img_filename = await self.save_image(img_url)
            if img_filename:
                replace_tag(img_element, f"##IMAGE## {img_filename}")

    async def get_custom_links(self, soup, url):
        return []

    async def get_standard_links(self, soup, url):
        return [(link, urljoin(url, link['href'])) for link in soup.find_all('a', href=True) if link.get('href')]


    async def get_links(self, soup, url):
        links = await self.get_standard_links(soup, url)
        links.append(await self.get_custom_links(soup, url))
        return links

    async def replace_with_linked_content(self, soup, linked_content, link_url, link_element):
        wrapper = soup.new_tag('div')
        wrapper['class'] = 'embedded-content'

        start = f"\n\n##START_LINKED_CONTENT_FROM: {link_url}\n"
        wrapper.append(BeautifulSoup(start, "html.parser"))

        wrapper.append(BeautifulSoup(linked_content, "html.parser"))

        end = f"\n##END_LINKED_CONTENT_FROM: {link_url}\n\n"
        wrapper.append(BeautifulSoup(end, "html.parser"))

        replace_tag(link_element, wrapper)
        return wrapper


    async def process_page(self, url, filename=None, current_depth=0, check_duplicates_depth=-1):
        """
        Обрабатывает отдельную страницу: получает контент, обрабатывает изображения и ссылки, сохраняет в Markdown.
        """
        if current_depth > self.max_depth:
            logging.debug(f"Превышена максимальная глубина для {url}, пропуск.")
            return ""
        if url in self.visited and check_duplicates_depth >= current_depth:
            logging.debug(f"Уже посещена {url}, пропуск.")
            return ""
        self.visited.add(url)
        logging.info(f"Обработка: {url}. Глубина {current_depth}")
        html = await self.retriever.retrieve_content(url)
        if not html:
            return ""

        soup = BeautifulSoup(html, 'html.parser')
        await self.remove_ignored_elements(soup, url)

        # Обработка навигационных элементов
        navigators = await self.get_navigators(soup, url)

        # Удаление дублирующихся элементов
        #await self.remove_duplicates(soup, url) # Не понятно, нужно ли

        # Обработка изображений
        if not self.no_images:
            await self.save_images(soup, url)

        # Обработка ссылок для рекурсивного обхода
        if urlparse(url).netloc == self.base_netloc:
            links = await self.get_links(soup, url)
            for link in links:
                link_element = link[0]
                link_url = link[1]
                if not link_url.startswith(('http://', 'https://')) or link_url == url:
                    continue
                if not has_ignored_class(link_element, self.non_recursive_classes) and link_url not in self.visited:
                    linked_content = await self.process_page(link_url, filename=filename, current_depth=current_depth + 1)
                    if linked_content:
                        new_element = await self.replace_with_linked_content(soup, linked_content, link_url, link_element)

        # Извлечение и обработка ссылок из навигационного элемента
        for nav in navigators:
            for a in nav.find_all('a', href=True):
                link_url = urljoin(url, a['href'])
                if link_url not in self.visited:
                    logging.info(f"Обрабатываю навигационную ссылку {a['href']} на {link_url}")
                    await self.process_navigation_link(link_url, current_depth=current_depth, filename=filename)

        content = str(soup)

        # Извлечение заголовка для метаданных
        title = soup.title.string.strip() if soup.title and soup.title.string else self.sanitize_filename(url)
        return content

    def html_to_markdown(self, soup):
        """
        Конвертирует HTML в Markdown с помощью markdownify.
        """
        html = str(soup)
        #markdown = md(html, heading_style="ATX")
        converter = html2text.HTML2Text()
        converter.body_width = 0
        markdown = converter.handle(html)
        markdown = markdown.strip() if markdown else ""
        markdown = markdown.replace('\t', ' ')
        markdown = re.sub(r'[ ]{2,}', ' ', markdown)
        markdown = re.sub(r'[ ]{1,}+\n{1,}', '\n', markdown)
        markdown = re.sub(r'\n{2,}', '\n', markdown)
        return markdown

    async def crawl(self, start_url):
        """
        Запускает процесс краулинга с заданного URL.
        """
        filename = self.sanitize_filename(start_url)
        start_tag = f"##START##: {start_url}\n\n"
        async with aiofiles.open(self.output_dir / filename, 'w', encoding='utf-8') as f:
            await f.write(start_tag)

        #content = await self.process_page(start_url, filename=filename)
        #markdown = self.html_to_markdown(content)
        #await self.save_markdown(filename, markdown)
        await self.process_navigation_link(start_url, filename=filename)
