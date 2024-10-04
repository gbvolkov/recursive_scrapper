import pandas as pd
import asyncio

from utils.kb_summariser import summarise
import logging

def process_refs(refs):
    return summarise(refs, max_length=512, min_length=64, do_sample=False)

async def main():
    # Задайте ваш стартовый URL
    df = pd.read_csv('./output/articles_data.csv', encoding="utf-8")
    df['solution'] = df['refs'].apply(process_refs)
    df.to_csv('./output/articles_data_summ.csv', index=False, header=True)


if __name__ == "__main__":
    asyncio.run(main())
