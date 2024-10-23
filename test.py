import pandas

import csv

import sys

# Увеличение лимита для поля
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)
        

import csv

with open('./output/articles_data_summ.csv', encoding="utf-8") as csvfile:
    reader = csv.reader(csvfile)
    for i, row in enumerate(reader):
        if i == 1231:  # Проверяем строку перед проблемной
            print("Строка 1232:", row)
        if i == 1232:
            print("Проблемная строка:", row)


pd1 = pandas.read_csv('./output/articles_data_summ.csv', encoding="utf-8")
#pd1 = pandas.read_csv('./output/articles_data_summ.csv', encoding="utf-8")
pd2 = pandas.read_csv('./output/kb.csv', encoding="utf-8")

#pd1 = pd1[pd1['refs'].notnull()]
#pd2 = pd2[pd2['refs'].notnull()]

#pd2['url'] = ''

pd = pandas.concat([pd1, pd2], ignore_index=True, sort=False)

pd['article_no'] = pd['no']
pd['no'] = range(1, len(pd) + 1)

pd = pd[['no', 'systems', 'problem', 'solution', 'samples', 'links', 'image_links', 'local_image_paths', 'refs', 'url', 'article_no']]
print(pd.info())

pd.to_csv('./output/kb_new.csv', index=False, encoding="utf-8") 
