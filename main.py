import os
import camelot
import pandas as pd 
import matplotlib
import matplotlib.pyplot as plt
import logging 
from unidecode import unidecode 
from configs.tools.postgres import PostgreSQLManager 
from configs.rules.notas import rules_dict

logging.basicConfig(level=logging.INFO)


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
        
        df= df.dropna(how='all')
        return df

    @staticmethod
    def fix_product_categories(df):
        if df.shape[0] > 1:
            second_line = df.iloc[1]
            if len(second_line) != 1:
                second_line = second_line[0].split(",")  # Quebra os valores da primeira coluna
                if len(second_line) == 3:
                    df["Subcategory"] = second_line[0].strip()
                    df["Category"] = second_line[1].strip()
                    df["Product_ID"] = second_line[2].strip()
                else:
                    df["Subcategory"] = None
                    df["Category"] = None
                    df["Product_ID"] = None
        return df 

    @staticmethod
    def fix_multiple_lines(df):
        return df.apply(lambda col: col.dropna().astype(str).str.cat(sep=' '), axis=0).to_frame().T

    @staticmethod
    def pivot_lines_to_columns(df):
        df.replace('', None, inplace=True)
        df = df.dropna(axis=1, how='all')
        df.columns = ['Key', 'Value']
        df_pivoted = df.set_index('Key').T
        df_pivoted.columns.name = None
        return df_pivoted

    def get_data(self, t_areas, t_cols, fix, fix_product =False, pivot=False):
        tables = camelot.read_pdf(
            self.path,
            pages=self.configs["pages"],
            flavor=self.configs["flavor"],
            table_areas=t_areas,
            columns=t_cols,
            strip_text=self.configs["strip_text"],
            passwords=self.configs["password"]
            )

        # if fix is true then fix header for every page in every tables
        if fix:
            for page in tables:
                table_content = self.fix_header(page.df)
        
        if pivot:
            for page in tables:
                table_content = self.pivot_lines_to_columns(page.df)
        
        if fix_product:
            for page in tables:
                table_content = self.fix_product_categories(page.df)    
        
        # if there is more than 1 table then add to the table_content if not return the first only
        result = pd.concat(table_content, ignore_index=True) if len(table_content) > 1 else table_content[0]
        
        return result


    def save_csv(self, df, file_name):
        if not os.path.exists(self.csv_path):
            os.makedirs(self.csv_path, exist_ok=True) 
        
        path = os.path.join(self.csv_path,f"{file_name}.csv")
        df.to_csv(path, sep=";", index=False)
    
    def add_infos(self, header, content):
        infos = header.iloc[0]
        df = pd.DataFrame([infos.values]*len(content), columns=header.columns)
        content = pd.concat([content.reset_index(drop=True),df.reset_index(drop=True)], axis=1) 
        content["created_at"] = pd.to_datetime("today")
        return content
        
    
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
        pass 

    def run(self):
        logging.info(f"Extracting data from {self.file_name}")
        header = self.get_data(self.configs['header_tables_areas'], self.configs['header_tables_cols'], self.configs['header_fix'])
        
        main = self.get_data(self.configs['table_areas'], self.configs['table_cols'], self.configs['fix'])
        small = self.get_data(self.configs['small_table_areas'], self.configs['small_table_cols'], self.configs['small_fix'])
        
        main = self.add_infos(header,main)
        if self.configs["small_sanitize"]:
            small = self.sanitize_column_names(small)

        main = self.sanitize_columns_names(main)
        small = self.sanitize_columns_names(small)
        
        logging.info(f"Saving csv - {self.file_name}")
        
        self.save_csv(main, self.file_name)
        self.save_csv(small, f"{self.file_name}_small")

        logging.info(f"Sending to DB - {self.file_name}")
        self.send_to_db(main, f"Fatura_{self.configs['name']}".lower())
        self.send_to_db(small, f"Fatura_{self.configs['name']}_small".lower())
        
        return {"main": main, "small": small}


def list_files(folder):
    try:
        files = [os.path.splitext(f)[0] for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        return files
    except FileNotFoundError:
        logging.info(f"A pasta '{folder}' não foi encontrada.")
        return []
    except Exception as e:
        logging.info(f"Ocorreu um erro: {e}")
        return []

if __name__ == "__main__":
    store = 'superstore'
    path = os.path.abspath(f"files/{store}/")
    files = list_files(path)
    
    for file in files:
        extractor = PDFDataExtractor(file, configs=rules_dict[store]).run()
    logging.info("Todos os arquivos foram processados")