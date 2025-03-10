import os
import camelot
import pandas as pd 
import matplotlib.pyplot as plt
import logging 
from unidecode import unidecode 
from configs.tools.postgres import PostgreSQLManager 
from configs.rules.notas import rules_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("main.log"), logging.StreamHandler()],
)


class PDFDataExtractor:
    def __init__(self, file_name, configs):
        self.path = os.path.abspath(f"files/{configs["name"].lower()}/{file_name}.pdf")
        self.csv_path = os.path.abspath("files/csv_invoices/")
        self.file_name = file_name 
        self.configs = configs 

    @staticmethod
    def fix_header(df):
        df.columns = df.iloc[0]
        df = df.drop(0)

        if df.shape[1] > 1:
            df = df.drop(df.columns[0], axis=1)
        
        df= df.dropna(axis=1,how='all')
        return df

    def get_data(self, t_areas, t_cols, fix)->pd.DataFrame:
        try:
            tables = camelot.read_pdf(
                self.path,
                pages=self.configs["pages"],
                flavor=self.configs["flavor"],
                table_areas=t_areas,
                columns=t_cols,
                # strip_text=self.configs["strip_text"],
                password=self.configs["password"]
                )
            if not tables.n:
                logging.warning(f"No tables found {self.file_name}")
                return pd.DataFrame()  # Retorna um DataFrame vazio

            table_content = [self.fix_header(page.df) if fix else page.df for page in tables]

            result = pd.concat(table_content, ignore_index=True) if len(table_content) > 1 else table_content[0]
            result = result.dropna(axis=1,how='all')
            return result
        except Exception as e:
            logging.error(f"Error getting data {e} --- {self.file_name}")

    def add_infos(self, invoice_id_df, df):
        try:
            if invoice_id_df.empty or df.empty:
                logging.warning(f"No relevat data detected {self.file_name}")
                return df  # Retorna o próprio DataFrame vazio
            
            invoice_id = invoice_id_df.iloc[0, 0]

            df["invoice_id"] = invoice_id
            df["created_at"] = pd.to_datetime("today")
            return df
        except Exception as e:
            logging.error(f"Error adding infos {e} --- {self.file_name}")


    def save_csv(self, df, file_name):
        if df is None or df.empty:
            logging.warning(f"{file_name}.csv failt to save as df is empty.")
            return
        
        if not os.path.exists(self.csv_path):
            os.makedirs(self.csv_path, exist_ok=True) 
        
        path = os.path.join(self.csv_path,f"{file_name}.csv")
        df.to_csv(path, sep=";", index=False)
    
    def sanitize_column_names(self, df):
        df.columns = df.columns.map(lambda x: unidecode(x))
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace(r'\W', '', regex=True)
        df.columns = df.columns.str.lower()
        return df

    @staticmethod
    def send_to_db(df, table_name):
        try:
            #open connection
            connection = PostgreSQLManager.alchemy()
            df.to_sql(table_name, connection, if_exists="append",index=False)
            logging.info(f"Data inserted into {table_name}")
        except Exception as e:
            logging.error(e)

    def run(self):
        logging.info(f"Extracting data from {self.file_name}")
        
        invoice_id = self.get_data(self.configs['invoice_area'], self.configs["invoice_columns"],self.configs["invoice_fix"])
        
        order_id = self.get_data(self.configs['order_id_area'], self.configs["order_id_columns"], self.configs['order_id_fix'])

        whom = self.get_data(self.configs['whom_area'], self.configs['whom_columns'], self.configs['whom_fix'])  
        
        header = self.get_data(self.configs['header_areas'], self.configs['header_columns'], self.configs['header_fix'])
        
        main = self.get_data(self.configs['table_areas'], self.configs['table_columns'], self.configs['fix'])
        
        small = self.get_data(self.configs['small_table_areas'], self.configs['small_columns'], self.configs['small_fix'])
        
        whom = self.add_infos(invoice_id, whom) 
        header = self.add_infos(invoice_id, header)
        main = self.add_infos(invoice_id,  main)
        small = self.add_infos(invoice_id, small)
        order = self.add_infos(invoice_id, order_id)
        
        
        self.save_csv(whom, f"{self.file_name}_whom") 
        logging.info(f"Saved csv - {self.file_name}_whom")
        
        self.save_csv(header, f"{self.file_name}_header")
        logging.info(f"Saved csv - {self.file_name}_header")
        
        self.save_csv(main, self.file_name)
        logging.info(f"Saved csv - {self.file_name}")
        
        self.save_csv(small, f"{self.file_name}_small")
        logging.info(f"Saved csv - {self.file_name}_small")
        
        self.save_csv(order, f"{self.file_name}_order")
        logging.info(f"Saved csv - {self.file_name}_order")
        
        return {"main": main, "small": small}


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

if __name__ == "__main__":
    store = 'superstore'
    path = os.path.abspath(f"files/{store}/")
    files = list_files(path)
    
    for file in files:
        extractor = PDFDataExtractor(file, configs=rules_dict[store]).run()
    logging.info("All files processed!")