import asyncio
from utils.retriever import IHTMLRetriever, IWebCrawler, replace_tag

async def main():
    # Задайте ваш стартовый URL
    start_url = "https://plantpad.samlab.cn/diseases_type.html?type=fungus&disease=black_spot_mixed_with_net_blotch"

    async with IHTMLRetriever(base_url=start_url) as retriever:
        crawler = IWebCrawler(
            retriever,
            duplicate_tags=['div', 'p', 'table'],
            no_images=False,
            max_depth=3,
            non_recursive_classes=['tag'],
            navigation_classes=['menus'],  # Ваши навигационные классы
            ignored_classes = ['header']
        )
        crawler.initialize()
        await crawler.crawl(start_url)

if __name__ == "__main__":
    asyncio.run(main())
