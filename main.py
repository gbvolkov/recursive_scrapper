import asyncio
import os
import re
import aiofiles
import hashlib  # Added for hashing
from urllib.parse import urljoin, urlparse
from pathlib import Path
from uuid import uuid4
import re

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup, NavigableString
from markdownify import markdownify as md

import logging

# Configure logging at the start of your script
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
    Cleans the tag by unwrapping insignificant sub-tags, resolving relative links,
    normalizing whitespace, and returning the cleaned HTML.

    :param tag: BeautifulSoup Tag object to clean.
    :param insignificant_tags: List of tag names to unwrap.
    :param base_url: The base URL to resolve relative links.
    :return: A string representation of the cleaned tag for hashing.
    """
    # Create a copy to avoid modifying the original tag
    tag_copy = tag.__copy__()

    # Unwrap insignificant sub-tags (remove the tag but keep its inner text)
    for sub_tag in tag_copy.find_all(insignificant_tags):
        sub_tag.unwrap()

    contains_link = False
    # Resolve all <a> tag hrefs to absolute URLs
    for a_tag in tag_copy.find_all('a', href=True):
        contains_link = True
        original_href = a_tag['href']
        a_tag['href'] = urljoin(base_url, original_href)

    if contains_link:
        # Get the cleaned outer HTML of the tag
        cleaned_html = re.sub(r'\s+', ' ', str(tag_copy).strip()).strip()
    else:
        cleaned_html = ""

    return cleaned_html

def get_header(headers, key):
    """
    Retrieve a header value case-insensitively.

    :param headers: The headers dictionary.
    :param key: The header key to retrieve.
    :return: The header value if found; otherwise, an empty string.
    """
    return next((value for k, value in headers.items() if k.lower() == key.lower()), '')

def replace_tag(tag, replacement_text):
    # Replace newline characters with <br/> to preserve line breaks in HTML
    replacement_html = replacement_text.replace('\n', '<br/>')
    # Parse the replacement HTML
    replacement_fragment = BeautifulSoup(replacement_html, 'html.parser')
    tag.replace_with(replacement_fragment)

def has_ignored_class(tag, ignored_classes):
    """
    Check if the tag has any class present in ignored_classes.

    :param tag: BeautifulSoup Tag object.
    :param ignored_classes: List of class names to ignore.
    :return: True if any class is in ignored_classes, False otherwise.
    """
    tag_classes = tag.get('class', [])  # Get the list of classes; default to empty list
    return any(cls in ignored_classes for cls in tag_classes)


def replace_a_tag_text(a_tag, new_text):
    """
    Replace the internal text of an <a> tag with new_text.
    
    :param a_tag: BeautifulSoup Tag object representing the <a> tag.
    :param new_text: String containing the new text to replace the existing text.
    """
    #if not a_tag.name == 'a':
    #    raise ValueError("The provided tag is not an <a> tag.")
    
    # Check if the <a> tag has a direct string
    if a_tag.string and isinstance(a_tag.string, NavigableString):
        a_tag.string.replace_with(new_text)
    else:
        # If there are multiple text nodes or nested tags, iterate and replace
        for content in a_tag.contents:
            if isinstance(content, NavigableString):
                content.replace_with(new_text)
            elif content.name:  # If the content is a tag (e.g., <span>, <strong>)
                # Recursively replace text in nested tags
                replace_a_tag_text(content, new_text)


USER_AGENT = "ILCrawler/1.0 (+http://gbvolkoff.name/crawler)"


class IHTMLRetriever:
    def __init__(self, base_url, login_url=None, login_credentials=None, user_agent=None):
        """
        Initialize the HTML Retriever.

        :param base_url: The base URL to start crawling from.
        :param login_url: The URL to perform login.
        :param login_credentials: A dict containing login credentials.
        :param user_agent: Custom User-Agent string.
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
        Perform login using the provided login URL and credentials.
        """
        if not self.login_url:
            return  # No login required
        try:
            await self.page.goto(self.login_url)
            # Customize selectors as needed
            await self.page.fill('input#username', self.login_credentials.get('username', ''))
            await self.page.fill('input#password', self.login_credentials.get('password', ''))
            await self.page.click('button#login')  # Assume there's a login button with id 'login'
            # Wait for navigation or some element that signifies successful login
            await self.page.wait_for_load_state('networkidle')
            logging.info(f"Successfully logged in at {self.login_url}")
        except Exception as e:
            logging.error(f"Login failed at {self.login_url}: {e}")

    async def retrieve_content(self, url):
        """
        Retrieve the HTML content of the given URL.

        :param url: The URL to retrieve.
        :return: HTML content as a string.
        """
        try:
            response = await self.page.goto(url, timeout=30000)  # 30 seconds timeout
            if response is None:
                logging.warning(f"No response for {url}")
                return ""
            status = response.status
            if status >= 400:
                logging.warning(f"Received status {status} for {url}")
                return ""
            content_type = get_header(response.headers, 'Content-Type').lower()
            if 'text/html' not in content_type:
                logging.warning(f"Skipping non-HTML content: {url}")
                return ""
            content = await self.page.content()
            return content
        except Exception as e:
            logging.error(f"Failed to retrieve {url}: {e}")
            return ""

class WebCrawler:
    def __init__(self, retriever, output_dir='output', images_dir='images', duplicate_tags=None, no_images=False, max_depth=5, non_recursive_classes=[]):
        """
        Initialize the WebCrawler.

        :param retriever: An instance of IHTMLRetriever.
        :param output_dir: Directory to store markdown files.
        :param images_dir: Directory to store images.
        :param duplicate_tags: List of HTML tags to check for duplicates.
        """
        self.retriever = retriever
        self.output_dir = Path(output_dir)
        self.no_images = no_images
        self.images_dir = self.output_dir / images_dir
        self.visited = set()
        self.base_netloc = urlparse(retriever.base_url).netloc
        self.max_depth = max_depth
        self.non_recursive_classes = non_recursive_classes

        # Set to track processed element hashes
        self.processed_elements = set()

        # List of tags to check for duplicates
        self.duplicate_tags = duplicate_tags or ['div', 'p', 'table']

        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)

    def sanitize_filename(self, url):
        """
        Sanitize the URL to create a valid filename.

        :param url: The URL to sanitize.
        :return: A sanitized filename string.
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
        Save the markdown content to a file with YAML front matter.

        :param filename: The filename to save as.
        :param content: The markdown content.
        :param title: Optional title for the front matter.
        :param url: Optional URL for the front matter.
        """
        if content is None:
            content = ""
        file_path = self.output_dir / filename
        front_matter = f"---\nTITLE: \"{title or filename}\"\nurl: \"{url or ''}\"\n---\n\n" if title or url else ""
        async with aiofiles.open(file_path, 'a', encoding='utf-8') as f:
            await f.write(front_matter + content + "\n\n===================================\n\n")

    async def save_image(self, img_url, retries=3, delay=2):
        """
        Download and save the image with retry mechanism.

        :param img_url: The image URL.
        :param retries: Number of retry attempts.
        :param delay: Delay between retries in seconds.
        :return: The local image filename.
        """
        for attempt in range(1, retries + 1):
            try:
                if img_url.startswith('data:'):
                    logging.warning(f"Skipping data URI image: {img_url}")
                    return ""
                response = await self.retriever.page.goto(img_url, timeout=10000)  # 10 seconds timeout
                if response is None:
                    logging.warning(f"Failed to download image: {img_url}")
                    return ""
                content_type = get_header(response.headers, 'Content-Type').lower()
                if not any(ct in content_type for ct in ['image/jpeg', 'image/png', 'image/gif', 'image/webp']):
                    logging.warning(f"Skipping non-image content: {img_url}")
                    return ""
                img_bytes = await response.body()
                # Determine image extension
                parsed = urlparse(img_url)
                ext = os.path.splitext(parsed.path)[1]
                if not ext:
                    ext = '.png'  # Default extension
                # Create a unique image filename using hash to prevent conflicts
                img_content_hash = hashlib.md5(img_bytes).hexdigest()
                img_name = re.sub(r'[\\/*?:"<>|]', "_", parsed.path.strip("/").replace("/", "_"))
                if not img_name:
                    img_name = "image"
                img_filename = f"{img_name}_{img_content_hash}{ext}"
                img_path = self.images_dir / img_filename
                # Save the image
                async with aiofiles.open(img_path, 'wb') as f:
                    await f.write(img_bytes)
                logging.info(f"Saved image: {img_filename}")
                return img_filename
            except Exception as e:
                logging.error(f"Attempt {attempt} - Error saving image {img_url}: {e}")
                if attempt < retries:
                    logging.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Failed to save image after {retries} attempts: {img_url}")
                    return ""

    async def process_page(self, url, filename=None, current_depth=0):
        """
        Process a single page: retrieve content, handle images and links, and save as markdown.

        :param url: The URL to process.
        :param filename: Optional filename for saving the markdown content.
        :param current_depth: Current depth level in the crawl.
        :return: The markdown content as a string.
        """
        if current_depth > self.max_depth:
            logging.debug(f"Depth limit exceeded for {url}, skipping.")
            return ""
        #if url in self.visited:
        #    logging.debug(f"Already visited {url}, skipping.")
        #    return ""
        self.visited.add(url)
        logging.info(f"Processing: {url}")
        html = await self.retriever.retrieve_content(url)
        if not html:
            return ""

        soup = BeautifulSoup(html, 'html.parser')

        tags_to_decompose = []
        # **Handle Duplicate Elements**
        #if "here are only two ways to live your life" in str(soup):
        #    print("FOUND!")
        for tag in soup.find_all(self.duplicate_tags):
            # Clean the tag to extract significant content
            # Compute hash of the tag's outer HTML
            tag_content = str(tag)
            cleaned_content = clean_tag_for_hashing(tag, INSIGNIFICANT_TAGS, base_url=url)
            if not cleaned_content or cleaned_content == '':
                continue
            tag_hash = hashlib.md5(cleaned_content.encode('utf-8')).hexdigest()

            if tag_hash in self.processed_elements:
                # Remove the duplicate element from the soup
                tags_to_decompose.append(tag)
                #tag.decompose()
                logging.debug(f"Duplicate element {tag_content} to be scipped in {url}")
            else:
                # Add the hash to the set of processed elements
                self.processed_elements.add(tag_hash)
                logging.debug(f"Processed new element in {url}")

        for tag in tags_to_decompose:
            tag.decompose()
            logging.debug(f"Skipped duplicate element in {url}")

        #if "here are only two ways to live your life" in str(soup):
        #    print("FOUND!")

        if not self.no_images:
            # **Handle Images**
            for img in soup.find_all('img'):
                src = img.get('src')
                if not src:
                    continue
                img_url = urljoin(url, src)
                img_filename = await self.save_image(img_url)
                if img_filename:
                    # Replace the img tag with ##IMAGE## tag
                    replace_tag(img, f"##IMAGE## {img_filename}")
                    #img.replace_with(f"##IMAGE## {img_filename}")

        # **Handle Links**
        if urlparse(url).netloc == self.base_netloc: # Process links for only same domain
            for a in soup.find_all('a', href=True):
                href = a['href']
                link_url = urljoin(url, href)
                # Only process HTTP and HTTPS links within the same domain
                if not link_url.startswith(('http://', 'https://')):
                    continue
                if link_url == url:
                    continue  # Skip self-referencing links
                
                if not has_ignored_class(a, self.non_recursive_classes):
                    # Recursively process the linked page
                    linked_content = await self.process_page(link_url, filename=filename, current_depth=current_depth + 1)
                    if linked_content:
                        # Replace the link with ##LINK## followed by the linked content
                        #replaced_object = BeautifulSoup(linked_content, 'html.parser')
                        #replacement_text = '\n'.join(replaced_object.stripped_strings) #self.html_to_markdown(linked_content)
                        #replace_a_tag_text(a, replacement_text)
                        replacement_text = f"<div>{linked_content}</div>"
                        replace_tag(a, replacement_text)


        # Convert the modified HTML to markdown
        #content = f"##LINK##: {url}\n\n"
        content = str(soup) #self.html_to_markdown(soup)
        #content += "\n======================================\n\n"

        # Extract the title for metadata
        title = soup.title.string.strip() if soup.title and soup.title.string else self.sanitize_filename(url)

        # Save the markdown file
        if filename is None:
            filename = self.sanitize_filename(url)
        #markdown = f"##LINK##: {url}\n\n"
        #markdown = self.html_to_markdown(soup)
        #markdown += "\n======================================\n\n"
        #await self.save_markdown(filename, markdown, title=title, url=url)
        return content

    def html_to_markdown(self, soup):
        """
        Convert BeautifulSoup HTML to markdown using markdownify.

        :param soup: BeautifulSoup object.
        :return: Markdown string.
        """
        html = str(soup)
        markdown = md(html, heading_style="ATX")
        markdown = markdown.strip() if markdown else ""
        markdown = markdown.replace('\t', ' ')
        # Replace multiple spaces with a single space
        markdown = re.sub(r'[ ]{2,}', ' ', markdown)
        markdown = re.sub(r'[ ]{1,}+\n{1,}', '\n', markdown)
        markdown = re.sub(r'\n{2,}', '\n', markdown)
        return markdown

    async def crawl(self, start_url):
        """
        Start crawling from the start_url.

        :param start_url: The URL to start crawling from.
        """
        filename = self.sanitize_filename(start_url)
        start_tag = f"##START##: {start_url}\n\n"
        async with aiofiles.open(self.output_dir / filename, 'w', encoding='utf-8') as f:
            await f.write(start_tag)

        content = await self.process_page(start_url, filename=filename)
        markdown = self.html_to_markdown(content)
        # Append the final markdown content
        await self.save_markdown(filename, markdown)

async def main():
    #start_url = "http://books.toscrape.com/"  # Replace with your start URL
    start_url = "http://quotes.toscrape.com/"
    login_url = "https://example.com/login"  # Replace with your login URL if needed
    login_credentials = {
        "username": "your_username",
        "password": "your_password"
    }

    # Initialize the retriever without login
    async with IHTMLRetriever(base_url=start_url, user_agent=USER_AGENT) as retriever:
        # If login is required, uncomment the following lines:
        # await retriever.login()
        crawler = WebCrawler(
            retriever,
            duplicate_tags=['div', 'p', 'table'],
            no_images=True,
            max_depth=4,
            non_recursive_classes = ['tag']
            # Removed max_depth and max_concurrent
        )
        await crawler.crawl(start_url)

if __name__ == "__main__":
    asyncio.run(main())
