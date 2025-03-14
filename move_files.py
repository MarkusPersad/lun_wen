from src.lun_wen.file_operation import copy_files_by_keyword
if __name__ == "__main__":
    copy_files_by_keyword(source_dir="calculated_data/1km", dest_dir="data/1km", suffix=".tif", key_words=["CHE", "SU", "ID", "TXx", "TXn"])
