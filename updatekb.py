import pandas as pd
import asyncio

from utils.kb_summariser import summarise, summarise_ya
import logging
import re

def summarise_text(text):
    cleaned_text = re.sub(r'##IMAGE##\s+\S+\.(png|jpg|jpeg|gif)', '', text)

    return summarise_ya(cleaned_text, max_length=1024, min_length=64, do_sample=False)


def process_csv_chunked(input_path, output_path, chunk_size=4096, overlap=0.35, skiprows=None):
    with pd.read_csv(input_path, chunksize=1, encoding="utf-8", skiprows=skiprows) as reader:
        # Determine the chunk size and overlap size (35%)
        
        for chunk in reader:
            refs = chunk['refs'].iloc[0]  # Access the value in the 'refs' column

            overlap_size = int(chunk_size * overlap)

            # Generate overlapping chunks
            start = 0
            end = 0
            while end < len(refs):
                end = start + chunk_size
                text_chunk = refs[start:end]

                # Create a summary for the chunk
                summary = summarise_text(text_chunk)

                # Create a new row with the original chunk values and add the summary
                new_row = chunk.copy()
                new_row['solution'] = summary

                # Append the new row to the output CSV
                new_row.to_csv(output_path, mode='a', index=False, header=not pd.io.common.file_exists(output_path))

                # Move the start forward by chunk size minus the overlap
                start += chunk_size - overlap_size


def process_csv(input_path, output_path, chunk_size=4096, overlap=0.35, skiprows=None):
    with pd.read_csv(input_path, chunksize=1, encoding="utf-8", skiprows=skiprows) as reader:
        # Determine the chunk size and overlap size (35%)
        
        for chunk in reader:
            refs = chunk['refs'].iloc[0]  # Access the value in the 'refs' column

            overlap_size = int(chunk_size * overlap)

            # Generate overlapping chunks
            start = 0
            end = 0
            while end < len(refs):
                end = start + chunk_size
                text_chunk = refs[start:end]

                # Create a summary for the chunk
                summaries = summarise_ya(text_chunk)
                for summary in summaries:
                    new_row = chunk.copy()
                    new_row['solution'] = summary['summary']
                    new_row['problem'] = summary['topic']
                    print(f'for Record NO: {chunk['no'].iloc[0]}: {summary["topic"]}: {summary["summary"]}')
                    new_row.to_csv(output_path, mode='a', index=False, header=not pd.io.common.file_exists(output_path))

                # Move the start forward by chunk size minus the overlap
                start += chunk_size - overlap_size




async def main():
    process_csv('./output/articles_data.csv', './output/articles_data_summ.csv', chunk_size=8192, overlap=0.5)#, skiprows=range(1,451))

if __name__ == "__main__":
    asyncio.run(main())
