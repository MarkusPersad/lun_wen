from pathlib import Path
import xarray
import os
from dask.diagnostics.progress import ProgressBar

def get_etccdi_stats(nc_path:str, var_name:str,shapefile:str,output_dir:str,chunks :int | dict =512):
    """
    参数:
    nc_path : NetCDF文件路径
    var_name : 要分析的变量名
    返回: 包含统计值的字典
    """
    ds = xarray.open_dataset(nc_path, chunks=chunks,engine='h5netcdf')
    if var_name not in ds.data_vars :
        raise ValueError(f"Variable '{var_name}' not found in dataset")
    valid_mask = ds[var_name].count(dim='valid_time') > 0
    path = Path(nc_path)


    with ProgressBar():
        txx = ds[var_name].max(dim="valid_time").compute()
        txx.rio.to_raster(os.path.join(output_dir, f"{path.name.split('.')[0]}-TXx.tif"))
        txn = ds[var_name].min(dim="valid_time").compute()
        txn.rio.to_raster(os.path.join(output_dir, f"{path.name.split('.')[0]}-TXn.tif"))
        freeze_days = (ds[var_name] < 0).sum(dim="valid_time").where(valid_mask).compute()
        freeze_days.rio.to_raster(os.path.join(output_dir, f"{path.name.split('.')[0]}-ID.tif"))
        summer_days = (ds[var_name] > 250).sum(dim="valid_time").where(valid_mask).compute()
        summer_days.rio.to_raster(os.path.join(output_dir, f"{path.name.split('.')[0]}-SU.tif"))
