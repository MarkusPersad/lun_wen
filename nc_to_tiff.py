from lun_wen import translate_data
import os

if __name__ == "__main__":
    for file in os.listdir('data'):
        if file.endswith('.nc'):
            translate_data.nc_to_tiff(os.path.join('data', file),shapefile="shp/China.shp",output_dir="0.25")
