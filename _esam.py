
import pandas as pd
import numpy as np


pd.options.mode.chained_assignment = None  # default='warn'

filename = "2024_10"

df = pd.read_csv("./input/" + filename + ".csv")

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
df_filtered.loc[index_mask, 'date'] = df_filtered.loc[index_mask, "A"].map(
    lambda x: x.replace('Data', '').strip())

df_filtered.loc[:, 'values'] = df_filtered.apply(
    lambda row: str(row['A']).split(" ")[1:] if str(row['A']).startswith('Razem') else None, axis=1)

df_filtered.loc[:, 'mpal'] = df_filtered['values'].apply(
    lambda x: x[0] if isinstance(x, list) and len(x) > 0 else np.nan)

# %%
df_filtered.loc[:, 'stops'] = df_filtered['values'].apply(
    lambda x: x[5] if isinstance(x, list) and len(x) > 5 else np.nan)

df_filtered['reg_number'] = df_filtered['reg_number'].replace('', np.nan)
df_filtered['reg_number'] = df_filtered['reg_number'].ffill()

df_filtered['date'].replace('', np.nan, inplace=True)
df_filtered['date'] = df_filtered['date'].ffill()

df_filtered.dropna(inplace=True)


df_filtered = df_filtered.drop_duplicates(subset=['values'])

df_agg_check = df_filtered.groupby(['reg_number', 'date']).agg({
    'stops': 'size',
}).reset_index()

pivot_stops_checks = df_agg_check.pivot(index="reg_number", columns="date", values="stops")

pivot_stops_checks.fillna(0, inplace=True)

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

df_filtered['mpal'] = df_filtered['mpal'].str.replace(',', '.').astype(float)
df_filtered['stops'] = df_filtered['stops'].str.replace(',', '.').astype(int)


df_agg = (df_filtered
          .groupby(['reg_number', 'date'])
          .agg({'stops': 'sum', })
          .reset_index())

df_agg['date'] = pd.to_datetime(df_agg['date'])

pivot_stops = df_agg.pivot(
    index='date',
    columns='reg_number',
    values='stops')

full_date_range = pd.date_range(start='2024-09-01', end='2024-09-30', freq='D')

pivot_stops = pivot_stops.reindex(full_date_range)
pivot_stops = pivot_stops.fillna(0)

grouped = df_filtered.groupby(['reg_number', 'date'])['mpal'].agg(list).reset_index()

max_length = grouped['mpal'].apply(len).max()

mpal_expanded_columns = pd.DataFrame(grouped['mpal'].tolist(), columns=[f'mpal_{i + 1}' for i in range(max_length)])

mpal_expanded_columns = mpal_expanded_columns.fillna(value='')

result_mpalperday = pd.concat([grouped[['reg_number', 'date']], mpal_expanded_columns], axis=1)


df_agg2 = df_filtered.groupby(['reg_number', 'date']).agg({
    'mpal': 'sum'
}).reset_index()


pivot_mpal = df_agg2.pivot(index='date', columns='reg_number', values='mpal')
pivot_mpal = pivot_mpal.fillna(0)
result_df = df_filtered.groupby('reg_number')[['mpal', 'stops']].sum()

with pd.ExcelWriter('./excel/' + filename + '.xlsx') as writer:
    result_df.to_excel(writer, sheet_name="mpal_stop")
    pivot_mpal.to_excel(writer, sheet_name="mpal")
    result_mpalperday.to_excel(writer, sheet_name="mpal-perday")
    pivot_stops.to_excel(writer, sheet_name="stops")
    stylized_df.to_excel(writer, sheet_name="del_rec")


