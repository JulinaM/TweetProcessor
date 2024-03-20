import json
import pandas as pd
import traceback, os, sys

def read_json_in_chunks(file_path, chunk_size=10):
    with open(file_path, 'r') as file:
        chunk = []
        for line in file:
            try:
                j = json.loads(line)
                try:
                    if j['delete']:
                        continue
                except Exception as e1:
                    lang = j['lang']
                    if lang == 'en':
                        row = {'created_at': j['created_at'],
                               'id': j['id'], 
                               'text': j['text'],
                               'user_id':j['user']['id'], 
                               'followers':j['user']['followers_count']
                              }
                    chunk.append(row)
            except:
                # Handle invalid JSON, skip or log the error
                # traceback.print_exc()
                break

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

        

# Function to preprocess each chunk of data
def preprocess_chunk(chunk):
    df = pd.DataFrame(chunk)
    df.drop_duplicates(subset='text',inplace=True)
    print(df.shape)
    return df

def processs(input_folder, output_folder, chunk_size):
        main_path = os.path.join(input_folder)
        for filename in os.listdir(main_path):
            if filename.endswith('.json'):
                filepath = os.path.join(main_path, filename)
                print('>'*40, filepath)
                try:
                    output_filename = output_folder + filename[0:10]+'_EN.csv' 
                    if os.path.exists(output_filename):
                        print(f"The file '{output_filename}' exists.")
                        continue

                    df_n = pd.DataFrame()
                    for chunk in read_json_in_chunks(filepath, chunk_size):
                        chunk_df = preprocess_chunk(chunk)
                        df_n = df_n._append(chunk_df, ignore_index=True)
                    df_n.to_csv(output_filename)
                    print(output_filename, ':: ', df_n.shape)
                    print(f"Done for {filename}!" )
                except:
                    traceback.print_exc()
                    # break
                    pass

#usage:: python3 filterEN.py /data2/julina/scripts/tweets/2020/03/ /data2/julina/scripts/tweets/2020/03/EN_CSV/

if __name__ == "__main__":
    try:
        input_folder = sys.argv[1]
        output_folder = sys.argv[2]
        print(':' * 50, input_folder, output_folder, 1000000)
        processs(input_folder, output_folder, 1000000)
    except:
        traceback.print_exc()
        print("missing arguments!!!!")
        exit(0)  
        
