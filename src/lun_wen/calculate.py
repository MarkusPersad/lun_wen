from pathlib import Path
import xarray as xr
import os
from dask.diagnostics.progress import ProgressBar
from numba import jit
import numpy as np

def get_etccdi_stats(nc_path:str, var_name:str,output_dir:str,chunks :int | dict =512):
    """
    参数:
    nc_path : NetCDF文件路径
    var_name : 要分析的变量名
    返回: 包含统计值的字典
    """
    ds = xr.open_dataset(nc_path, chunks=chunks,engine='h5netcdf')
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
        ds.close()


def calculate_heatwave_events(nc_path: str, out_dir: str, temp_var: str = 'tm', threshold: float = 35.0, min_duration: int = 6, chunks: dict = {"valid_time": 100}):
    """
    计算连续高温事件（≥阈值温度且持续≥指定天数）

    参数：
    nc_path      : 输入NetCDF文件路径
    out_dir      : 输出目录
    temp_var     : 温度变量名（默认为'tm'）
    threshold    : 高温阈值（单位：℃，默认35）
    min_duration : 最小持续天数（默认6）
    chunks       : Dask分块策略（默认时间分块100）
    """
    path = Path(nc_path)
    @jit(nopython=True)
    def detect_events(arr,thresold,min_duration=6):
        """
        检测连续高温事件：返回事件次数
        """
        events = 0
        current_length = 0
        for val in arr:
            if np.isnan(val):
                return np.nan
            if val > thresold:
                current_length += 1
                if current_length == min_duration:
                    events += 1
            else:
                current_length = 0
        return events
    with xr.open_dataset(nc_path,chunks=chunks,engine="h5netcdf") as ds:
        ds = ds.chunk({'valid_time': -1})  # 合并valid_time为单一块
        if temp_var not in ds.data_vars:
            raise ValueError(f"Variable '{temp_var}' not found in dataset")
        with ProgressBar():
            che = xr.apply_ufunc(detect_events,
                ds[temp_var],
                input_core_dims=[['valid_time']],
                kwargs={"thresold":threshold,"min_duration":min_duration},
                vectorize=True,
                dask="parallelized",
                output_dtypes=[float]).compute()
        che.rio.to_raster(os.path.join(out_dir, f"{path.name.split('.')[0]}-CHE.tif"))
        ds.close()
