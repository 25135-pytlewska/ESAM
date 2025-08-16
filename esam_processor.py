import pandas as pd
import numpy as np
from typing import List, Dict, Union
from pathlib import Path

class ESAMProcessor:
    def __init__(self, input_dir: str = "./input"):
        self.input_dir = Path(input_dir)
        pd.options.mode.chained_assignment = None

    def read_data(self, filename: str) -> pd.DataFrame:
        """Read and initialize the CSV data with standard column names."""
        file_path = self.input_dir / f"{filename}.csv"
        df = pd.read_csv(file_path)
        # return df.set_axis(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', "L", "M", "N", "O"], axis=1)
        return df.set_axis(['A'], axis=1)
        
    def filter_relevant_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter rows containing specific keywords."""
        keywords = ['Nr rej', 'Data ', 'Razem']
        mask = np.logical_or.reduce([df["A"].str.contains(word, na=False) for word in keywords])
        return df[mask][['A']]

    @staticmethod
    def extract_reg_number(text: str) -> str:
        """Extract registration number from text."""
        if str(text).startswith("Nr rej. "):
            return text.replace("Nr rej. ", "")
        return text

    def process_registration_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean registration numbers."""
        df = df.copy()
        df['reg_number'] = df["A"].apply(self.extract_reg_number)
        df['reg_number'] = df['reg_number'].str.replace('^Data.*', '', regex=True)
        df['reg_number'] = df['reg_number'].str.replace('^Razem.*', '', regex=True)
        df['reg_number'] = df['reg_number'].replace('', np.nan)
        return df

    def process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and extract dates."""
        df = df.copy()
        mask = df["A"].str.startswith('Data')
        df.loc[mask, 'date'] = df.loc[mask, "A"].map(lambda x: x.replace('Data', '').strip())
        df['date'] = df['date'].replace('', np.nan)
        return df

    def process_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and extract mpal and stops values."""
        df = df.copy()
        df['values'] = df.apply(
            lambda row: str(row['A']).split(" ")[1:] if str(row['A']).startswith('Razem') else None, 
            axis=1
        )
        df['mpal'] = df['values'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else np.nan)
        df['stops'] = df['values'].apply(lambda x: x[5] if isinstance(x, list) and len(x) > 5 else np.nan)
        return df

    def clean_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and convert numeric columns."""
        df = df.copy()
        df['mpal'] = df['mpal'].str.replace(',', '.').astype(float)
        df['stops'] = df['stops'].str.replace(',', '.').astype(int)
        return df

    def process_data(self, filename: str) -> Dict[str, pd.DataFrame]:
        """Main processing function that returns all required dataframes."""
        df = self.read_data(filename)
        df_filtered = self.filter_relevant_rows(df)
        
        df_filtered = self.process_registration_numbers(df_filtered)
        df_filtered = self.process_dates(df_filtered)
        df_filtered = self.process_values(df_filtered)
        
        # Forward fill registration numbers and dates
        df_filtered['reg_number'] = df_filtered['reg_number'].ffill()
        df_filtered['date'] = df_filtered['date'].ffill()
        
        df_filtered.dropna(inplace=True)
        df_filtered = df_filtered.drop_duplicates(subset=['values'])
        
        df_filtered = self.clean_numeric_columns(df_filtered)
        
        return {
            'filtered_data': df_filtered,
            'summary': self.create_summary(df_filtered),
            'daily_mpal': self.create_daily_mpal(df_filtered),
            'daily_stops': self.create_daily_stops(df_filtered),
            'mpal_per_day': self.create_mpal_per_day(df_filtered)
        }

    def save_to_excel(self, filename: str, data_dict: Dict[str, pd.DataFrame], output_dir: str = './excel'):
        """Save all dataframes to Excel file."""
        output_path = Path(output_dir) / f"{filename}.xlsx"
        with pd.ExcelWriter(output_path) as writer:
            data_dict['summary'].to_excel(writer, sheet_name="mpal_stop")
            data_dict['daily_mpal'].to_excel(writer, sheet_name="mpal")
            data_dict['mpal_per_day'].to_excel(writer, sheet_name="mpal-perday")
            data_dict['daily_stops'].to_excel(writer, sheet_name="stops")
            self.create_styled_checks(data_dict['filtered_data']).to_excel(writer, sheet_name="del_rec")

    @staticmethod
    def create_summary(df: pd.DataFrame) -> pd.DataFrame:
        """Create summary dataframe with total mpal and stops per registration number."""
        return df.groupby('reg_number')[['mpal', 'stops']].sum()

    @staticmethod
    def create_daily_mpal(df: pd.DataFrame) -> pd.DataFrame:
        """Create daily mpal pivot table."""
        df_agg = df.groupby(['reg_number', 'date'])['mpal'].sum().reset_index()
        pivot = df_agg.pivot(index='date', columns='reg_number', values='mpal')
        return pivot.fillna(0)
    

    @staticmethod
    def create_daily_stops(df: pd.DataFrame) -> pd.DataFrame:
        """Create daily stops pivot table."""
        df_agg = df.groupby(['reg_number', 'date'])['stops'].sum().reset_index()
        df_agg['date'] = pd.to_datetime(df_agg['date'])
        pivot = df_agg.pivot(index='date', columns='reg_number', values='stops')
        
        # full_date_range = pd.date_range(start='2024-09-01', end='2024-09-30', freq='D')
        # pivot = pivot.reindex(full_date_range)
        return pivot.fillna(0)

    @staticmethod
    def create_mpal_per_day(df: pd.DataFrame) -> pd.DataFrame:
        """Create detailed mpal per day breakdown."""
        grouped = df.groupby(['reg_number', 'date'])['mpal'].agg(list).reset_index()
        max_length = grouped['mpal'].apply(len).max()
        
        mpal_columns = pd.DataFrame(
            grouped['mpal'].tolist(), 
            columns=[f'mpal_{i + 1}' for i in range(max_length)]
        ).fillna('')
        
        return pd.concat([grouped[['reg_number', 'date']], mpal_columns], axis=1)

    def create_styled_checks(self, df: pd.DataFrame) -> pd.DataFrame.style:
        """Create styled checks dataframe."""
        df_agg_check = df.groupby(['reg_number', 'date']).agg({
            'stops': 'size',
        }).reset_index()
        
        pivot = df_agg_check.pivot(index="reg_number", columns="date", values="stops")
        pivot.fillna(0, inplace=True)
        
        return pivot.style.map(self._color_cells)

    @staticmethod
    def _color_cells(value: float) -> str:
        """Helper function for cell coloring."""
        colors = {
            0: 'orange',
            1: 'red',
            2: 'green'
        }
        color = colors.get(value, 'blue' if value > 2 else 'white')
        return f'background-color: {color}' 