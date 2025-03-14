from pathlib import Path
from lun_wen import translate_data
import os
if __name__ == "__main__":
    path = Path('data/1km')
    folders = [f.name for f in path.glob('*') if f.is_dir()]
    for _,folder in enumerate(folders):
        translate_data.rename_file(os.path.join('data/1km', folder))
