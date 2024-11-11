from esam_processor import ESAMProcessor

processor = ESAMProcessor()
results = processor.process_data("2024_10")
processor.save_to_excel("2024_10", results) 