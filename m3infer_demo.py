import csv
import json
from collections import OrderedDict
from m3inference.m3inference import M3Inference
import pprint
import os

m3 = M3Inference(use_full_model=False) # see docstring for details

def make_decision(data):
    decisions = {}
    for key, attributes in data.items():
        decision = {}
        for attr, probs in attributes.items():
            decision[attr] = max(probs, key=probs.get)
        decisions[key] = decision
    return decisions

def update_ordered_dict_from_jsonl(data, jsonl_file_path):
    try:
        # Open and read the JSON Lines file
        with open(jsonl_file_path, 'r') as file:
            for line in file:
                # Parse each line as JSON
                json_line = json.loads(line)

                # Extract the 'id' from the JSON line
                json_id = json_line.get('id')

                # Check if the id exists in your OrderedDict
                if json_id in data:
                    # Merge the JSON line data with the corresponding entry in the OrderedDict
                    for key, value in json_line.items():
                        if key != 'id':  # Avoid overwriting the 'id' key
                            if key in data[json_id]:
                                # Update existing keys with new values
                                data[json_id][key].update(value)
                            else:
                                # Add new keys to the entry
                                data[json_id][key] = value
    except:
        print("An error occurred!")

    return data

def save_csv(decisions, out_filepath):
    data = []
    for key, value in decisions.items():
        value['id'] = key
        data.append(value)

    # Define the CSV file headers, including the new 'id' column
    headers = ['id', 'gender', 'age', 'org', 'text', 'user_id', 'name', 'screen_name', 'description', 'lang']
    # Write data to CSV
    with open(out_filepath, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for item in data:
            writer.writerow(item)

def infer_all_jsonl(directory_path, out_path):
    for filename in os.listdir(directory_path):
        if filename.endswith('.jsonl'):
            jsonl_filepath = os.path.join(directory_path, filename)
            fn = filename[:-6]
            out_filepath = os.path.join(out_path, fn+'.csv')
            if os.path.exists(out_filepath):
                print(f"file for {filename} already exists. Skipping...")
                continue
            pred = m3.infer(jsonl_filepath) 
            decisions = make_decision(pred)
            updated_data = update_ordered_dict_from_jsonl(decisions, jsonl_filepath)
            save_csv(updated_data, out_filepath)
            print(f"Processed {filename} .")

directory_path = '/data2/julina/scripts/tweets/2020/03/user_csv/temp'
out_path = '/data2/julina/scripts/tweets/2020/03/user_csv/demo'

infer_all_jsonl(directory_path, out_path)

print("All JSONL files have been infered successfully.")