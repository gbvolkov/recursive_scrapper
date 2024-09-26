#from markdownify import markdownify as md
from bs4 import BeautifulSoup, NavigableString
import html2text



with open('input/source.html', 'r', encoding='utf-8') as f:
    src_content = f.read()

with open('input/replaced.html', 'r', encoding='utf-8') as f:
    replaced_content = f.read()

converter = html2text.HTML2Text()
converter.body_width = 0
markdown = converter.handle(src_content)
with open('input/source.md', 'w', encoding='utf-8') as f:
    src_content = f.write(markdown)

converter = html2text.HTML2Text()
converter.body_width = 0
markdown = converter.handle(replaced_content)
with open('input/replaced.md', 'w', encoding='utf-8') as f:
    src_content = f.write(markdown)
