import os
import pandas as pd
from tqdm import tqdm
from filelock import FileLock
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, as_completed

# Define the source and destination directories
INPUT_DIR = os.path.join(r'C:\Users\e0638886\Desktop\ExtractNotes', 'notes')
OUTPUT_DIR = os.path.join(r'C:\Users\e0638886\Desktop\ExtractNotes', 'tables')
missing_notes_path = os.path.join(OUTPUT_DIR, 'missing_notes.csv')
lock_file_path = missing_notes_path + '.lock'
MAX_WORKERS = 60

def extract_tables(input_path, output_dir, debug=False, is_save=False):
    try:
        with open(input_path, 'r', encoding='utf-8') as input_file:
            html_content = input_file.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        if not tables:
            return None
        os.makedirs(output_dir, exist_ok=True)
        count = 1
        for table in tables:
            with open(os.path.join(output_dir, f'Table{count}.html'), 'w', encoding='utf-8') as f:
                f.write(str(table) + '\n\n\n')
                count += 1
    except Exception as e:
        print((str(e)))
        with FileLock(lock_file_path):
            with open(missing_notes_path, 'a', encoding='utf-8') as file:
                file.write(f"{input_path}\n")

def process_file(input_path):
    filename = os.path.splitext(os.path.basename(input_path))[0]
    extension = os.path.splitext(os.path.basename(input_path))[1]
    relative_path = os.path.relpath(input_path, INPUT_DIR).split('.')[0]
    output_path = os.path.join(OUTPUT_DIR, relative_path)
    try:
        if (extension == '.html' or extension == '.htm'):
            if os.path.isdir(output_path):
                return None
            extract_tables(input_path, output_path)
    except Exception as e:
        print(str(e))
        return filename, error_message

def run():
    all_files = [os.path.join(root, file)
                 for root, dirs, files in os.walk(INPUT_DIR)
                 for file in files]
    error_results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(process_file, input_path): input_path for input_path in all_files}
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc='Processing files'):
            result = future.result()
            if result:
                error_results.append(result)
    if error_results:
        file_path = os.path.join(OUTPUT_DIR, 'item_results_error.csv')
        error_df = pd.DataFrame(error_results, columns=['name', 'error'])
        lock_file_path = file_path + '.lock'
        with FileLock(lock_file_path):
            if os.path.exists(file_path):
                existing_error_df = pd.read_csv(file_path)
                updated_error_df = pd.concat([existing_error_df, error_df], ignore_index=True).drop_duplicates(
                    subset=['name', 'error'], keep='first')
            else:
                updated_error_df = error_df.drop_duplicates(subset=['name', 'error'], keep='first')
            updated_error_df.to_csv(file_path, index=False)

if __name__ == '__main__':
    run()