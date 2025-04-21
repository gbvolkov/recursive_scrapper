from utils.retriever import IHTMLRetriever, IWebCrawler, replace_tag
import asyncio
import pandas as pd


async def main():
    df = pd.read_csv("data.csv", encoding='utf-8')
    print(df.columns)

if __name__ == "__main__":
    asyncio.run(main())
