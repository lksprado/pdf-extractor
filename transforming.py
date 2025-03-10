import pandas as pd 
import os 
import logging
import glob

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("transforming.log"), logging.StreamHandler()],
)

def list_files(folder):
    try:
        files = [os.path.splitext(f)[0] for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        return files
    except FileNotFoundError:
        logging.info(f"Dir '{folder}' not found.")
        return []
    except Exception as e:
        logging.info(f"Something went wrong: {e}")
        return []

def parse_main_table(file):
    try:
        df = pd.read_csv(file, sep = ';')
        df = df.dropna(axis=1, how='all')
        df.columns = df.columns.str.strip().str.lower()
        num_linhas = df.shape[0]
        if num_linhas == 3:
            item = ' '.join(df['item'].iloc[0:2])
            df.at[0, 'item'] = item
            df = df.drop(1).reset_index(drop=True)
        second_line = df.iloc[1]
        df = df.drop(1).reset_index(drop=True)
        second_line = second_line.iloc[0].split(',')
        df['subcategory'] = second_line[0].strip()
        df['category'] = second_line[1].strip()
        df['item_id'] = second_line[2].strip()
        df['invoice_id'] = df['invoice_id'].str.extract(r'(\d+)').astype(int)
        df['quantity'] = df['quantity'].astype(str).str.extract(r'(\d+)').astype(int)
        df['rate'] = df['rate'].str.replace(r'[^\d.]', '', regex=True).astype(float)
        df['amount'] = df['amount'].str.replace(r'[^\d.]', '', regex=True).astype(float)
        df = df[['created_at','invoice_id','item_id','category','subcategory','item', 'quantity', 'rate', 'amount']] 
        return df
    except Exception as e:
        logging.error(e)
        return pd.DataFrame()

def parse_header(file):
    try:
        df = pd.read_csv(file, sep = ';')
        df = df.dropna(axis=1, how='all')
        df = df.pivot_table(index=["invoice_id", "created_at"], columns='1', values='3', aggfunc="first").reset_index()
        df.columns.name = None  # Removendo nome das colunas
        df.columns = df.columns.str.strip().str.lower().str.replace(' ','_').str.replace(':','')
        df['balance_due'] = df['balance_due'].str.replace(r'[^\d.]', '', regex=True).astype(float)
        df['invoice_id'] = df['invoice_id'].str.extract(r'(\d+)').astype(int)
        df = df.drop(columns=['created_at'])
        return df
    except Exception as e:
        logging.error(e)
        return pd.DataFrame()

def parse_order(file):
    try:
        df = pd.read_csv(file, sep = ';')
        order_id = df.iloc[2,0]
        order_id = order_id.split(':')[-1].strip()
        invoice_id = df.iloc[0,1]
        df = pd.DataFrame({"order_id":[order_id], "invoice_id":[invoice_id]})
        df['invoice_id'] = df['invoice_id'].str.extract(r'(\d+)').astype(int)
        return df 
    except Exception as e:
        logging.error(e)
        return pd.DataFrame()

def parse_small(file):
    try:
        df = pd.read_csv(file, sep = ';')
        df = df.dropna(axis=1, how='all')
        df = df.pivot_table(index=["invoice_id", "created_at"], columns='1', values='2', aggfunc="first").reset_index()
        df.columns.name = None  # Removendo nome das colunas
        df.columns = df.columns.str.strip().str.lower().str.replace(' ','_').str.replace(':','')
        df.columns = df.columns.to_series().replace({col: "discount" for col in df.columns if "discount" in col})
        df['invoice_id'] = df['invoice_id'].str.extract(r'(\d+)').astype(int)
        numeric_cols = ['subtotal', 'discount', 'shipping', 'total']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'[^\d.]', '', regex=True).astype(float) 
            else:
                df[col] = None
        df = df.drop(columns=['created_at'])
        return df 
    except Exception as e:
        logging.error(e)
        return pd.DataFrame()

def parse_whom(file):
    try:
        df = pd.read_csv(file, sep = ';')
        df.columns = df.columns.str.strip().str.lower().str.replace(' ','_').str.replace(':','')
        df['invoice_id'] = df['invoice_id'].str.extract(r'(\d+)').astype(int)
        invoice_id = df['invoice_id'].iloc[0]
        bill_to = df['bill_to'].dropna().iloc[0] if df['bill_to'].notna().any() else None
        ship_to = ', '.join(df['ship_to'].dropna())
        df = pd.DataFrame({
            'invoice_id': [invoice_id],
            'bill_to': [bill_to],
            'ship_to': [ship_to]
        })
        return df 
    except Exception as e:
        logging.error(e)
        return pd.DataFrame()

def run():
    folder = 'csv_invoices' 
    path = os.path.abspath(f"files/{folder}/")
    files = list_files(path)
    PATTERN_MAIN = f'invoice_*_*.csv'
    PATTERN_HEADER = f'invoice_*_*_header.csv'
    PATTERN_ORDER = 'invoice_*_*_order.csv'
    PATTERN_WHOM = 'invoice_*_*_whom.csv'
    PATTERN_SMALL = 'invoice_*_*_small.csv'

    consolidated_dfs = {
        'header': [],
        'order': [],
        'whom': [],
        'small': [],
        'main': []
    }

    for file in files:
        filename = os.path.basename(file)+'.csv'
        filename_path = os.path.join(path,filename)
        df = pd.DataFrame()  # DataFrame vazio para cada arquivo

        if glob.fnmatch.fnmatch(filename, PATTERN_HEADER):
            logging.info(f"Processando header: {filename}")
            df = parse_header(filename_path)
            consolidated_dfs['header'].append(df)
        elif glob.fnmatch.fnmatch(filename, PATTERN_ORDER):
            logging.info(f"Processando order: {filename}")
            df = parse_order(filename_path)
            consolidated_dfs['order'].append(df)
        elif glob.fnmatch.fnmatch(filename, PATTERN_WHOM):
            logging.info(f"Processando whom: {filename}")
            df = parse_whom(filename_path)
            consolidated_dfs['whom'].append(df)
        elif glob.fnmatch.fnmatch(filename, PATTERN_SMALL):
            logging.info(f"Processando small: {filename}")
            df = parse_small(filename_path)
            consolidated_dfs['small'].append(df)
        elif glob.fnmatch.fnmatch(filename, PATTERN_MAIN):
            logging.info(f"Processando main table: {filename}")
            df = parse_main_table(filename_path)
            consolidated_dfs['main'].append(df)
        else:
            logging.warning(f"File not found: {filename_path}")
            continue

        if not df.empty:
            logging.info(f"File {filename} processed!")
        else:
            logging.warning(f"Error {filename}, DataFrame is empty.")

        if df.empty:
            logging.warning(f"Error {filename}, DataFrame is empty.")

    # Unificar DataFrames de cada categoria
    for key in consolidated_dfs:
        if consolidated_dfs[key]:  # Se houver DataFrames na lista
            consolidated_dfs[key] = pd.concat(consolidated_dfs[key], ignore_index=True)
        else:
            consolidated_dfs[key] = pd.DataFrame()  # Evitar erro ao acessar depois

    # Merge final
    final_df = consolidated_dfs['main']
    for key in ['header', 'order', 'whom', 'small']:
        if not consolidated_dfs[key].empty:
            final_df = final_df.merge(consolidated_dfs[key], on='invoice_id', how='outer', suffixes=('', f'_{key}'))

    logging.info("Final merge succesfuly!")
    
    final_df.to_csv('all_invoices.csv',sep=";",index=False)

if __name__ == "__main__":
    # run()
    # t = parse_header('/media/lucas/Files/2.Projetos/pdf-extractor/files/csv_invoices/invoice_Greg Tran_22023_header.csv')
    # print(t)
    file1 = '/media/lucas/Files/2.Projetos/pdf-extractor/files/csv_invoices/invoice_Aaron Bergman_36258_small.csv'
    file2 = '/media/lucas/Files/2.Projetos/pdf-extractor/files/csv_invoices/invoice_Alan Shonely_31900.csv'
    t = parse_small(file1)
    t.to_csv('teste.csv',sep=';',index=False)