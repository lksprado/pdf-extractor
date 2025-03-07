import os
import camelot
import pandas as pd 
import matplotlib
import matplotlib.pyplot as plt

file_name = 'invoice_Aleksandra Gannaway_33911'
path = os.path.abspath(f"files/superstore/{file_name}.pdf")

def fix_header(df):
    df.columns = df.iloc[0]
    df = df.drop(0)

    if df.shape[1] > 1:
        df = df.drop(df.columns[0], axis=1)
    
    df= df.dropna(how='all')
    return df

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

def fix_multiple_lines(df):
    return df.apply(lambda col: col.dropna().astype(str).str.cat(sep=' '), axis=0).to_frame().T

def pivot_lines_to_columns(df):
    df.replace('', None, inplace=True)
    df = df.dropna(axis=1, how='all')
    df.columns = ['Key', 'Value']
    df_pivoted = df.set_index('Key').T
    df_pivoted.columns.name = None
    return df_pivoted

tables = camelot.read_pdf(
    path,
    pages='1-end', # page 1 to all
    flavor='stream',
    table_areas=['47, 299, 223, 270'],
    columns=['47, 270'],
    # strip_text='./n',
    # split_text=False,
    )

# print(tables[0].parsing_report) 
camelot.plot(tables[0], kind="contour")
plt.show()
print(tables[0].df )

table_content = [fix_header(table.df) for table in tables]
# table_content = [pivot_lines_to_columns(table.df) for table in tables]

# Concatenar todas as tabelas em um único DataFrame, se houver mais de uma
result = pd.concat(table_content, ignore_index=True) if len(table_content) > 1 else table_content[0]

print(result)
result.to_csv("teste.csv",index=False)