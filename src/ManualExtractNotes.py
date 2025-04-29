from bs4 import BeautifulSoup
import os
import pandas as pd

file = r'D:/GPTReporting/ACPAS/10K_Project/Scripts/Downloaded_Filings_10-K/raw/1467858/1467858_2019-02-06_10-K_html.html'
input_dir = r'D:\GPTReporting\ACPAS\10K_Project\Scripts\Downloaded_Filings_10-K\raw\1467858'
relative_path = os.path.relpath(file, input_dir)
output_dir = os.path.join(r'C:\Users\e0638886\Desktop\ExtractNotes', relative_path)
print(output_dir)
with open(file, 'r', encoding='utf-8') as input_file:
    html_content = input_file.read()
soup = BeautifulSoup(html_content, 'html.parser')

start_tag = soup.find_all('font', string=' 50')[0].parent.parent.next_sibling.next_sibling.next_sibling
end_tag = soup.find_all('font', string=' 97')[0].parent.parent

sibling_content = [str(start_tag)]

for sibling in start_tag.next_siblings:
    if sibling == end_tag:
        break
    sibling_content.append(str(sibling))

with open(output_dir, 'w', encoding='utf-8') as f:
    for content in sibling_content:
        f.write(content + '\n')