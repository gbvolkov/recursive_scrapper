import asyncio
import os
import re
import aiofiles
import hashlib  # Для хеширования
from urllib.parse import urljoin, urlparse
from pathlib import Path
from uuid import uuid4

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup, NavigableString
from markdownify import markdownify as md

import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

INSIGNIFICANT_TAGS = ['small', 'strong', 'em', 'span', 'b', 'i', 'u', 'sup', 'sub']

def clean_tag_for_hashing(tag, insignificant_tags, base_url):
    """
    Очищает тег для хеширования.
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

    #if contains_link:
    #    cleaned_html = re.sub(r'\s+', ' ', str(tag_copy).strip()).strip()
    #else:
    #    cleaned_html = ""

    return cleaned_html

def get_header(headers, key):
    """
    Получает значение заголовка независимо от регистра.
    """
    return next((value for k, value in headers.items() if k.lower() == key.lower()), '')

def replace_tag(tag, replacement_text):
    """
    Заменяет HTML-тег на заданный текст или HTML.
    """
    replacement_html = replacement_text.replace('\n', '<br/>')
    replacement_fragment = BeautifulSoup(replacement_html, 'html.parser')
    tag.replace_with(replacement_fragment)

def has_ignored_class(tag, ignored_classes):
    """
    Проверяет, содержит ли тег любой из игнорируемых классов.
    """
    tag_classes = tag.get('class', [])
    return any(cls in ignored_classes for cls in tag_classes)

def replace_a_tag_text(a_tag, new_text):
    """
    Заменяет внутренний текст <a> тега на новый текст.
    """
    if a_tag.string and isinstance(a_tag.string, NavigableString):
        a_tag.string.replace_with(new_text)
    else:
        for content in a_tag.contents:
            if isinstance(content, NavigableString):
                content.replace_with(new_text)
            elif content.name:
                replace_a_tag_text(content, new_text)

USER_AGENT = "ILCrawler/1.0 (+http://gbvolkoff.name/crawler)"

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
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context(user_agent=self.user_agent)
        self.page = await self.context.new_page()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def login(self):
        """
        Выполняет вход на сайт при необходимости.
        """
        if not self.login_url:
            return  # Вход не требуется
        try:
            await self.page.goto(self.login_url)
            # Настройте селекторы под конкретную страницу логина
            await self.page.fill('input#username', self.login_credentials.get('username', ''))
            await self.page.fill('input#password', self.login_credentials.get('password', ''))
            await self.page.click('button#login')  # Предполагается наличие кнопки с id 'login'
            await self.page.wait_for_load_state('networkidle')
            logging.info(f"Успешный вход на {self.login_url}")
        except Exception as e:
            logging.error(f"Не удалось выполнить вход на {self.login_url}: {e}")

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
            content_type = get_header(response.headers, 'Content-Type').lower()
            if 'text/html' not in content_type:
                logging.warning(f"Пропуск не-HTML контента: {url}")
                return ""
            content = await self.page.content()
            return content
        except Exception as e:
            logging.error(f"Не удалось получить {url}: {e}")
            return ""

class WebCrawler:
    def __init__(self, retriever, output_dir='output', images_dir='images', duplicate_tags=None,
                 no_images=False, max_depth=5, non_recursive_classes=[], navigation_classes=None,
                 ignored_classes=None):
        """
        Инициализация WebCrawler.
        """
        self.retriever = retriever
        self.output_dir = Path(output_dir)
        self.no_images = no_images
        self.images_dir = self.output_dir / images_dir
        self.visited = set()
        self.base_netloc = urlparse(retriever.base_url).netloc
        self.max_depth = max_depth
        self.non_recursive_classes = non_recursive_classes
        self.ignored_classes = ignored_classes or []

        # Отслеживание обработанных элементов
        self.processed_elements = set()

        # Навигационные классы
        self.navigation_classes = navigation_classes or []
        self.processed_navigation = set()

        # Список тегов для проверки дубликатов
        self.duplicate_tags = duplicate_tags or ['div', 'p', 'table']

        # Создание директорий для вывода и изображений
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)

    def sanitize_filename(self, url):
        """
        Санифицирует URL для использования в качестве имени файла.
        """
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_")
        if not path:
            path = "index"
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
                if not any(ct in content_type for ct in ['image/jpeg', 'image/png', 'image/gif', 'image/webp']):
                    logging.warning(f"Пропуск не-изображения: {img_url}")
                    return ""
                img_bytes = await response.body()
                parsed = urlparse(img_url)
                ext = os.path.splitext(parsed.path)[1]
                if not ext:
                    ext = '.png'  # Расширение по умолчанию
                img_content_hash = hashlib.md5(img_bytes).hexdigest()
                img_name = re.sub(r'[\\/*?:"<>|]', "_", parsed.path.strip("/").replace("/", "_"))
                if not img_name:
                    img_name = "image"
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
            self.visited.add(link_url)
            logging.info(f"Обработка навигационной ссылки: {link_url}")
            content = await self.process_page(link_url, filename=filename, current_depth=current_depth)
            markdown = self.html_to_markdown(content)
            await self.save_markdown(filename, markdown)

    async def process_page(self, url, filename=None, current_depth=0):
        """
        Обрабатывает отдельную страницу: получает контент, обрабатывает изображения и ссылки, сохраняет в Markdown.
        """
        if current_depth > self.max_depth:
            logging.debug(f"Превышена максимальная глубина для {url}, пропуск.")
            return ""
        #if url in self.visited:
        #    logging.debug(f"Уже посещена {url}, пропуск.")
        #    return ""
        #self.visited.add(url)
        logging.info(f"Обработка: {url} глубина {current_depth}")
        html = await self.retriever.retrieve_content(url)
        if not html:
            return ""

        soup = BeautifulSoup(html, 'html.parser')

        #Удаляем игнорируемые элементы
        for ignored_class in self.ignored_classes:
            ignored_elements = soup.find_all(class_=ignored_class)
            for ignored in ignored_elements:
                ignored.decompose()
                logging.debug(f"Удалён элемент с классом {ignored_class} из {url}")

        # Обработка навигационных элементов
        navigators = []
        for nav_class in self.navigation_classes:
            nav_elements = soup.find_all(class_=nav_class)
            for nav in nav_elements:
                hashed_content = clean_tag_for_hashing(nav, INSIGNIFICANT_TAGS, base_url=url)
                nav_hash = hashlib.md5(hashed_content.encode('utf-8')).hexdigest()
                if nav_hash not in self.processed_navigation:
                    logging.debug(f"Добавлен в очередь навигационный элемент с классом {nav_class} из {url}")
                    self.processed_navigation.add(nav_hash)
                    navigators.append(nav.__copy__())
                # Удаление навигационного элемента из содержимого страницы
                nav.decompose()
                logging.debug(f"Удалён навигационный элемент с классом {nav_class} из {url}")

        # Обработка дублирующихся элементов
        """
        tags_to_decompose = []
        for tag in soup.find_all(self.duplicate_tags):
            cleaned_content = clean_tag_for_hashing(tag, INSIGNIFICANT_TAGS, base_url=url)
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
        """
        
        # Обработка изображений
        if not self.no_images:
            for img in soup.find_all('img'):
                src = img.get('src')
                if not src:
                    continue
                img_url = urljoin(url, src)
                img_filename = await self.save_image(img_url)
                if img_filename:
                    replace_tag(img, f"##IMAGE## {img_filename}")

        # Обработка ссылок для рекурсивного обхода
        if urlparse(url).netloc == self.base_netloc:
            for a in soup.find_all('a', href=True):
                href = a['href']
                link_url = urljoin(url, href)
                if not link_url.startswith(('http://', 'https://')):
                    continue
                if link_url == url:
                    continue  # Пропуск самопосылки

                if not has_ignored_class(a, self.non_recursive_classes):
                    if link_url not in self.visited:
                        linked_content = await self.process_page(link_url, filename=filename, current_depth=current_depth + 1)
                        if linked_content:
                            replacement_text = f"<div>{linked_content}</div>"
                            replace_tag(a, replacement_text)

        # Извлечение и обработка ссылок из навигационного элемента
        for nav in navigators:
            for a in nav.find_all('a', href=True):
                link_url = urljoin(url, a['href'])
                if link_url not in self.visited:
                    logging.info(f"Обрабатываю навигационную ссылку {nav_class} на {link_url}")
                    await self.process_navigation_link(link_url, current_depth=current_depth, filename=filename)

        # Конвертация изменённого HTML в Markdown
        #content = self.html_to_markdown(soup)
        content = str(soup)

        # Извлечение заголовка для метаданных
        title = soup.title.string.strip() if soup.title and soup.title.string else self.sanitize_filename(url)

        # Сохранение Markdown файла
        #if filename is None:
        #    filename = self.sanitize_filename(url)
        #await self.save_markdown(filename, content, title=title, url=url)
        return content

    def html_to_markdown(self, soup):
        """
        Конвертирует HTML в Markdown с помощью markdownify.
        """
        html = str(soup)
        markdown = md(html, heading_style="ATX")
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

async def main():
    # Задайте ваш стартовый URL
    start_url = "http://books.toscrape.com/index.html"
    #start_url = "https://quotes.toscrape.com/page/1/"
    login_url = "https://example.com/login"  # Замените на ваш URL для входа, если необходимо
    login_credentials = {
        "username": "your_username",
        "password": "your_password"
    }

    # Инициализация retriever без логина
    async with IHTMLRetriever(base_url=start_url, user_agent=USER_AGENT) as retriever:
        # Если требуется логин, раскомментируйте следующие строки:
        # await retriever.login()
        crawler = WebCrawler(
            retriever,
            duplicate_tags=['div', 'p', 'table'],
            no_images=True,
            max_depth=1,
            non_recursive_classes=['tag'],
            navigation_classes=['side_categories', 'pager'],  # Ваши навигационные классы
            ignored_classes = ['footer', 'row header-box', 'breadcrumb', 'header container-fluid', 'icon-star', 'image_container']
        )
        await crawler.crawl(start_url)

if __name__ == "__main__":
    asyncio.run(main())
