import pandas as pd
import numpy as np

def process_file(file):
    """
    Process the uploaded file according to the specifications in esam.ipynb
    
    Args:
        file: FileStorage object from Flask
    Returns:
        pandas.DataFrame: Processed data
    """
    # Read the file based on its extension
    filename = file.filename.lower()
    if filename.endswith('.csv'):
        df = pd.read_csv(file)
    elif filename.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file)
    else:
        raise ValueError("Unsupported file format. Please upload a CSV or Excel file.")

    # TODO: Add your processing logic here based on esam.ipynb
    df = df.set_axis(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', "L", "M", "N", "O"], axis=1)
    df_filtered = df[
    np.logical_or.reduce([df["A"].str.contains(word, na=False) for word in ['Nr rej', 'Data ', 'Razem']])]
    
    df_filtered = df_filtered[['A']]
    df_filtered.loc[:, 'reg_number'] = np.nan

    def modify(row):
        if str(row).startswith("Nr rej. "):
            return row.replace("Nr rej. ", "")
        else:
            return row

    df_filtered.loc[:, 'reg_number'] = df_filtered["A"].apply(modify)
    df_filtered.loc[:, 'reg_number'] = df_filtered['reg_number'].str.replace('^Data.*', '', regex=True)
    df_filtered.loc[:, 'reg_number'] = df_filtered['reg_number'].str.replace('^Razem.*', '', regex=True)

    index_mask = df_filtered["A"].str.startswith('Data')
    df_filtered.loc[index_mask, 'date'] = df_filtered.loc[index_mask, "A"].map(lambda x: x.replace('Data', '').strip())
    df_filtered.loc[:, 'values'] = df_filtered.apply(lambda row: str(row['A']).split(" ")[1:] if str(row['A']).startswith('Razem') else None, axis=1)
    df_filtered.loc[:, 'mpal'] = df_filtered['values'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else np.nan)
    df_filtered.loc[:, 'stops'] = df_filtered['values'].apply(lambda x: x[5] if isinstance(x, list) and len(x) > 5 else np.nan)
    
    # Append reg_number to the dataframe
    df_filtered['reg_number'] = df_filtered['reg_number'].replace('', np.nan)
    df_filtered['reg_number'] = df_filtered['reg_number'].ffill()

    # Append date to the dataframe
    df_filtered['date'].replace('', np.nan, inplace=True)
    df_filtered['date'] = df_filtered['date'].ffill()

    df_filtered.dropna(inplace=True)

    # Drop duplicates - some of data is duplicated on few sites of PDF document
    df_filtered = df_filtered.drop_duplicates(subset=['values'])


    # Aggregate data by reg_number and date 
    df_agg_check = df_filtered.groupby(['reg_number', 'date']).agg({
        'stops': 'size',
    }).reset_index()

    pivot_stops_checks = df_agg_check.pivot(index="reg_number", columns="date", values="stops")

    pivot_stops_checks.fillna(0, inplace=True)
    pivot_stops_checks


    # Color cells based on the value
    def color_cells(value):
        if value == 0:
            color = 'orange'
        elif value == 2:
            color = 'green'
        elif value > 2:
            color = 'blue'
        elif value == 1:
            color = 'red'
        else:
            color = 'white'

        return f'background-color: {color}'

    stylized_df = pivot_stops_checks.style.map(color_cells)
    stylized_df

    df_filtered['mpal'] = df_filtered['mpal'].str.replace(',', '.').astype(float)
    df_filtered['stops'] = df_filtered['stops'].str.replace(',', '.').astype(int)

    df_agg = df_filtered.groupby(['reg_number', 'date']).agg({
        'stops': 'sum',
    }).reset_index()

    pivot_stops = df_agg.pivot(index='reg_number', columns='date', values='stops')
    # pivot_stops.columns = [f'{col}' for col in pivot_stops.columns]
    pivot_stops  = pivot_stops.fillna(0)


    df_agg2 = df_filtered.groupby(['reg_number', 'date']).agg({
        'mpal': 'sum'
    }).reset_index()

    pivot_mpal = df_agg2.pivot(index='reg_number', columns='date', values='mpal')
    # pivot_mpal.columns = [f'{col}' for col in pivot_mpal.columns]
    pivot_mpal = pivot_mpal.fillna(0)
    pivot_mpal

    result_df = df_filtered.groupby('reg_number')[['mpal', 'stops']].sum()

    with pd.ExcelWriter('./excel/'+filename+'.xlsx') as writer:
        result_df.to_excel(writer, sheet_name="jelcz_mpal_stopy")
        pivot_mpal.to_excel(writer, sheet_name="mpal")
        pivot_stops.to_excel(writer, sheet_name="stopy")
        stylized_df.to_excel(writer, sheet_name="dostawy_odbiory")
    # This is a placeholder for the actual processing
    processed_df = df_filtered  # Replace this with actual processing

    return processed_df