import cdsapi

def download_data(year:int):
    dataset = "derived-era5-single-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": ["2m_temperature"],
        "year": str(year),
        "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ],
        "day": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12",
            "13", "14", "15",
            "16", "17", "18",
            "19", "20", "21",
            "22", "23", "24",
            "25", "26", "27",
            "28", "29", "30",
            "31"
        ],
        "daily_statistic": "daily_maximum",
        "time_zone": "utc+08:00",
        "frequency": "1_hourly",
        "area": [54, 73, 4, 136] #中国经纬度范围
    }

    client = cdsapi.Client()
    output_file =f"{year}.nc"
    client.retrieve(dataset, request,output_file)

def download_era5_data(year:int,month:int):
    dataset = "derived-era5-land-daily-statistics"
    request = {
        "variable": ["2m_temperature"],
        "year": str(year),
        "month": str(month).zfill(2),
        "day": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12",
            "13", "14", "15",
            "16", "17", "18",
            "19", "20", "21",
            "22", "23", "24",
            "25", "26", "27",
            "28", "29", "30",
            "31"
        ],
        "daily_statistic": "daily_maximum",
        "time_zone": "utc+08:00",
        "frequency": "1_hourly",
        "area": [54, 73, 3, 136]
    }
    client = cdsapi.Client()
    try:
        client.retrieve(dataset, request, f"{year}_{month}.nc")
    except Exception:
        client.retrieve(dataset, request, f"{year}_{month}.nc")
