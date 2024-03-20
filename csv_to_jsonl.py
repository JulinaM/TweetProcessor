import csv
import json
import os

def clean_csv(input_csv_filepath, cleaned_csv_filepath):
    """Read the input CSV file and write a cleaned version to cleaned_csv_filepath, removing null bytes."""
    with open(input_csv_filepath, 'r', encoding='utf-8', errors='ignore') as input_file:
        with open(cleaned_csv_filepath, 'w', encoding='utf-8') as cleaned_file:
            for line in input_file:
                cleaned_line = line.replace('\0', '')  # Replace null bytes with nothing
                cleaned_file.write(cleaned_line)

def csv_to_jsonl_with_removals(input_csv_filepath, jsonl_filepath):
    cleaned_csv_filepath = input_csv_filepath + ".cleaned"
    clean_csv(input_csv_filepath, cleaned_csv_filepath)
    # fields_to_remove = ['created_at', '']
    fields_to_remove = ['']
    # Open the cleaned CSV file for reading
    with open(cleaned_csv_filepath, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        with open(jsonl_filepath, 'w') as jsonl_file:
            for row in csv_reader:
                for field in fields_to_remove:
                    row.pop(field, None)
                row['lang'] = 'en'
                jsonl_file.write(json.dumps(row) + '\n')
    os.remove(cleaned_csv_filepath)

def process_all_csv_in_directory(input_folder, output_folder):
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            input_file_path = os.path.join(input_folder, filename)
            output_filename = filename.replace('.csv', '.jsonl')
            output_file_path = os.path.join(output_folder, output_filename)
            if os.path.exists(output_file_path):
                print(f"JSONL file for {output_file_path} already exists. Skipping...")
                continue
            csv_to_jsonl_with_removals(input_file_path, output_file_path)
            print(f"Processed {filename} to JSONL.")

#usage:: python3 csv_to_jsonl.py /data2/julina/scripts/tweets/2020/03/user_csv/ /data2/julina/scripts/tweets/2020/03/user_csv/jsonl/ 

if __name__ == "__main__":
    try:
        input_folder = sys.argv[1]
        output_folder = sys.argv[2]
        print(':' * 50, input_folder, output_folder, 1000000)
        process_all_csv_in_directory(input_folder, output_folder, 1000000)
    except:
        traceback.print_exc()
        print("missing arguments!!!!")
        exit(0)  

print("All CSV files have been processed and converted to JSONL successfully.")