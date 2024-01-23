import pandas as pd 

dic = {
    'goMethods.csv': {
        'table_name': 'go_methods',
        'attr': {
            'Order method code': 'order_method_code',
            'Order method type': 'order_method_type'
        } 
    },
    'goProducts.csv': {
        'table_name': 'go_products',
        'attr': {
            'Product number': 'product_number',
            'Product line': 'product_line',
            'Product type': 'product_type',
            'Product': 'product',
            'Product brand': 'product_brand',
            'Product color': 'product_color',
            'Unit cost': 'unit_cost',
            'Unit price': 'unit_price'
        }
    },
    'goRetailers.csv': {
        'table_name': 'go_retailers',
        'attr': {        
            'Retailer code': 'retailer_code',
            'Retailer name': 'retailer_name',
            'Type': 'retailer_type',
            'Country': 'country',
        }
    },
    'goDailySales.csv': {        
        'table_name': 'go_daily_sales',
        'attr': {
            'Retailer code': 'retailer_code',
            'Product number': 'product_number',
            'Order method code': 'order_method_code',
            'Date': 'order_date',
            'Quantity': 'quantity', 
            'Unit price': 'unit_price',
            'Unit sale price': 'unit_sale_price',
        }
    }
}

csv_filenames = list(dic.keys())

baseDir = 'gosales_csv/'
df_list = [ pd.read_csv(baseDir + csv_filename) for csv_filename in csv_filenames ]
dfs = dict(zip(csv_filenames, df_list))
# print(dfs)

sql = 'USE go_sales;\n'

def extract_row(x: pd.Series):

    row = [ str(i) if type(i) != str else f'"{i}"' for i in x.to_list() ]
    str_row = ','.join(row)
    return f'\t({ str_row })'


for csv_filename in dfs:
    attrs = list(dic[csv_filename]['attr'].values())
    df = dfs[csv_filename]
    table_name = dic[csv_filename]['table_name']
    tmp = ','.join(attrs)

    sql += f'INSERT INTO { table_name } ({tmp}) \n'
    sql += f'\tVALUES '
    row_strs = df.apply(extract_row, axis=1)
    val = ',\n'.join(row_strs.to_list())
    sql += val

    sql += ';\n'


with open(f'{baseDir}/populate.sql', 'w') as f:
    f.write(sql)
