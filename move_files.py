from src.lun_wen.file_operation import copy_files_by_keyword
if __name__ == "__main__":
    copy_files_by_keyword(source_dir="calculated_data/0.25", dest_dir="data/0.25", suffix=".tif", key_words=["CHE", "SU", "ID", "TXx", "TXn"])
