import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from esam_processor import ESAMProcessor

@pytest.fixture
def processor():
    return ESAMProcessor()

@pytest.fixture
def sample_df():
    data = {
        'A': [
            'Nr rej. TK1503U',
            'Data 03-03-25 KM Stopy KM+Stopy+Dodatki Stawka Wartość',
            'Razem 13,4 0 4540 / 5367 4 583,99 PLN',
            'Nr rej. DEF456',
            'Data 2024-09-02',
            'Razem 15,3 x x x x 3'
        ]
    }
    return pd.DataFrame(data)

def test_extract_reg_number(processor):
    assert processor.extract_reg_number("Nr rej. TK1503U") == "TK1503U"
    assert processor.extract_reg_number("Something else") == "Something else"

def test_filter_relevant_rows(processor, sample_df):
    filtered = processor.filter_relevant_rows(sample_df)
    assert len(filtered) == 6
    assert list(filtered.columns) == ['A']

def test_process_registration_numbers(processor, sample_df):
    filtered = processor.filter_relevant_rows(sample_df)
    processed = processor.process_registration_numbers(filtered)
    
    assert 'reg_number' in processed.columns
    assert processed['reg_number'].iloc[0] == 'TK1503U'
    assert pd.isna(processed['reg_number'].iloc[1])  # Date row should be NaN

def test_process_dates(processor, sample_df):
    filtered = processor.filter_relevant_rows(sample_df)
    processed = processor.process_dates(filtered)
    
    assert 'date' in processed.columns
    assert processed.loc[1, 'date'] == '03-03-25'

def test_process_values(processor, sample_df):
    filtered = processor.filter_relevant_rows(sample_df)
    processed = processor.process_values(filtered)
    
    assert all(col in processed.columns for col in ['values', 'mpal', 'stops'])
    assert processed.loc[2, 'mpal'] == '13,4'
    assert processed.loc[2, 'stops'] == '4'

def test_clean_numeric_columns(processor):
    df = pd.DataFrame({
        'mpal': ['10,5', '15,3'],
        'stops': ['2', '3']
    })
    cleaned = processor.clean_numeric_columns(df)
    
    assert cleaned['mpal'].dtype == float
    assert cleaned['stops'].dtype == int
    assert cleaned['mpal'].iloc[0] == 10.5
    assert cleaned['stops'].iloc[0] == 2

def test_color_cells(processor):
    assert processor._color_cells(0) == 'background-color: orange'
    assert processor._color_cells(1) == 'background-color: red'
    assert processor._color_cells(2) == 'background-color: green'
    assert processor._color_cells(3) == 'background-color: blue'

# def test_create_summary(processor, sample_df):
#     filtered = processor.filter_relevant_rows(sample_df)
#     processed = processor.process_registration_numbers(filtered)
#     processed = processor.process_dates(processed)
#     processed = processor.process_values(processed)
#     processed = processor.clean_numeric_columns(processed)
    
#     summary = processor.create_summary(processed)
#     assert isinstance(summary, pd.DataFrame)
#     assert all(col in summary.columns for col in ['mpal', 'stops'])

# Integration test
# def test_full_processing(processor, tmp_path):
#     # Create a temporary CSV file
#     test_data = pd.DataFrame({
#         'A': ['Nr rej. ABC123', 'Data 2024-09-01', 'Razem 10,5 x x x x 2']
#     })
    
#     # Set up temporary input directory
#     input_dir = tmp_path / "input"
#     input_dir.mkdir()
#     test_data.to_csv(input_dir / "test_file.csv", index=False)
    
#     # Initialize processor with temporary directory
#     processor = ESAMProcessor(input_dir=str(input_dir))
    
#     # Process the data
#     results = processor.process_data("test_file")
    
#     assert isinstance(results, dict)
#     assert all(key in results for key in [
#         'filtered_data', 'summary', 'daily_mpal', 
#         'daily_stops', 'mpal_per_day'
#     ]) 