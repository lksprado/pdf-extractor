import os
import camelot
import pandas as pd 
import matplotlib
import matplotlib.pyplot as plt

file_name = 'invoice_Zuschuss Donatelli_18672'
path = os.path.abspath(f"files/superstore/{file_name}.pdf")

def fix_header(df):
    df.columns = df.iloc[0]
    df = df.drop(0)

    if df.shape[1] > 1:
        df = df.drop(df.columns[0], axis=1)
    
    df= df.dropna(how='all')
    return df

tables = camelot.read_pdf(
    path,
    pages='1-end', # page 1 to all
    flavor='stream',
    table_areas=['385, 480, 572, 385'],
    columns= ['385, 465, 570'],
    # strip_text='./n',
    # split_text=False,
    )

# print(tables[0].parsing_report) 
camelot.plot(tables[0], kind="contour")
plt.show()
print(tables[0].df )

table_content = [fix_header(table.df) for table in tables]

# # Concatenar todas as tabelas em um único DataFrame, se houver mais de uma
result = pd.concat(table_content, ignore_index=True) if len(table_content) > 1 else table_content[0]
