import pandas

pd1 = pandas.read_csv('./output/articles_data_summ.csv', encoding="utf-8")
pd2 = pandas.read_csv('./output/kb.csv', encoding="utf-8")

#pd1 = pd1[pd1['refs'].notnull()]
#pd2 = pd2[pd2['refs'].notnull()]

#pd2['url'] = ''

pd = pandas.concat([pd1, pd2], ignore_index=True, sort=False)

pd['no'] = range(1, len(pd) + 1)

pd = pd[['no', 'systems', 'problem', 'solution', 'samples', 'links', 'image_links', 'local_image_paths', 'refs', 'url']]
print(pd.info())

pd.to_csv('./output/kb_new.csv', index=False, encoding="utf-8") 
