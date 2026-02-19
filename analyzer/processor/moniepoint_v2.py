import pandas as pd
import numpy as np

def extract_transaction_moniepoint(pdf):

    header = ['Date', 'Narration', 'Reference', 'Debit', 'Credit', 'Balance']

    dataframes = []

    for page in pdf.pages:
        try:
            tables = page.extract_tables()
            if not tables or not tables[0]:
                continue

            table = tables[0]

            df_page = pd.DataFrame(table, columns=header)
            dataframes.append(df_page)

        except Exception as e:
            print(f" Error processing page: {e}")
            continue

    if not dataframes:
        print(" No valid tables found in document.")
        return pd.DataFrame()

    df = pd.concat(dataframes, ignore_index=True).replace("", np.nan).dropna(how="all")

    # --- Clean numeric and date fields ---
    df = df.dropna(subset=['Date'])
    #df['Date'] = df['Date'].str.split('T').split(",").str[0]

    return df