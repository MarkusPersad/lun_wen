
from lun_wen.download_data import download_era5_data


if __name__ == "__main__":
    # for year in range(2021, 2023):
    #     for month in range(1, 13):
    #         download_era5_data(year, month)
    for month in range(8, 13):
        download_era5_data(2022, month)
