from pathlib import Path
import xarray as xr
import os
from dask.diagnostics.progress import ProgressBar
from numba import jit
import numpy as np

@jit(nopython=True)
def _max(arr:np.ndarray):
    if np.isnan(arr).any():
        return np.nan
    max = arr[0]
    for value in arr:
        if value > max:
            max = value
    return max

@jit(nopython=True)
def _min(arr:np.ndarray):
    if np.isnan(arr).any():
        return np.nan
    min = arr[0]
    for value in arr:
        if value < min:
            min = value
    return min

@jit(nopython=True)
def _summerdays(arr:np.ndarray,threshold:float):
    if np.isnan(arr).any():
        return np.nan
    count = 0.0
    for value in arr:
        if value > threshold:
            count += 1
    return count

@jit(nopython=True)
def _winterdays(arr:np.ndarray,threshold:float):
    if np.isnan(arr).any():
        return np.nan
    count = 0.0
    for value in arr:
        if value < threshold:
            count += 1
    return count

@jit(nopython=True)
def _precentile(arr: np.ndarray,percentile:float):
    if np.isnan(arr).any():
        return np.nan
    percentValue = np.percentile(arr,percentile)
    return np.sum(arr > percentValue)

def get_etccdi_stats(nc_path:str, var_name:str,output_dir:str,chunks :int | dict =512,time_dim:str='valid_time'):
    """
    参数:
    nc_path : NetCDF文件路径
    var_name : 要分析的变量名
    返回: 包含统计值的字典
    """
    ds = xr.open_dataset(nc_path, chunks=chunks,engine='h5netcdf')
    if var_name not in ds.data_vars :
        raise ValueError(f"Variable '{var_name}' not found in dataset")
    # valid_mask = ds[var_name].count(dim=time_dim) > 0
    path = Path(nc_path)


    with ProgressBar():
        # txx = ds[var_name].max(dim="valid_time").compute()
        txx = xr.apply_ufunc(
            _max,
            ds[var_name],
            input_core_dims=[[time_dim]],
            vectorize=True,
            dask='parallelized',
            output_dtypes=[float]
        ).compute()
        txx.rio.to_raster(
            os.path.join(output_dir, f"{path.name.split('.')[0]}-TXx.tif"),
            dtype="float32",
            compress="LZW"
        )
        del txx
        # txn = ds[var_name].min(dim="valid_time").compute()
        txn = xr.apply_ufunc(
            _min,
            ds[var_name],
            input_core_dims=[[time_dim]],
            vectorize=True,
            dask='parallelized',
            output_dtypes=[float]
        ).compute()
        txn.rio.to_raster(
            os.path.join(output_dir, f"{path.name.split('.')[0]}-TXn.tif"),
            dtype="float32",
            compress="LZW"
        )
        del txn
        # txn.rio.to_raster(os.path.join(output_dir, f"{path.name.split('.')[0]}-TXn.tif"))
        # freeze_days = (ds[var_name] < 0).sum(dim="valid_time").where(valid_mask).compute()
        freeze_days = xr.apply_ufunc(
            _winterdays,
            ds[var_name],
            input_core_dims=[[time_dim]],
            kwargs={"threshold": 0},
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        ).compute()
        freeze_days.rio.to_raster(
            os.path.join(output_dir, f"{path.name.split('.')[0]}-ID.tif"),
            dtype="float32",
            compress="LZW"
        )
        del freeze_days
        # freeze_days.rio.to_raster(os.path.join(output_dir, f"{path.name.split('.')[0]}-ID.tif"))
        # summer_days = (ds[var_name] > 250).sum(dim="valid_time").where(valid_mask).compute()
        summer_days = xr.apply_ufunc(
            _summerdays,
            ds[var_name],
            input_core_dims=[[time_dim]],
            kwargs={"threshold": 250},
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float]
        ).compute()
        # summer_days.rio.to_raster(os.path.join(output_dir, f"{path.name.split('.')[0]}-SU.tif"))
        summer_days.rio.to_raster(
            os.path.join(output_dir, f"{path.name.split('.')[0]}-SU.tif"),
            dtype="float32",
            compress="LZW"
        )
        del summer_days
    ds.close()


def calculate_heatwave_events(nc_path: str, out_dir: str, temp_var: str = 'tm', threshold: float = 35.0, min_duration: int = 6, chunks: dict = {"valid_time": 100},time_dim:str="valid_time"):
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
        # ds = ds.chunk({'valid_time': -1})  # 合并valid_time为单一块
        if temp_var not in ds.data_vars:
            raise ValueError(f"Variable '{temp_var}' not found in dataset")
        with ProgressBar():
            che = xr.apply_ufunc(detect_events,
                ds[temp_var],
                input_core_dims=[[time_dim]],
                kwargs={"thresold":threshold,"min_duration":min_duration},
                vectorize=True,
                dask="parallelized",
                output_dtypes=[float]).compute()
        che.rio.to_raster(
            os.path.join(out_dir, f"{path.name.split('.')[0]}-CHE.tif"),
            dtype="float32",
            compress="LZW"
        )
        del che
        ds.close()

def tx90p_count(nc_path: str, out_dir: str, temp_var: str = 'tm', threshold: float = 35.0, chunks: dict = {"valid_time": 100},time_dim:str="valid_time"):
    """
    计算90%高温事件的个数
    参数：
        nc_path      : NetCDF文件路径
        out_dir      : 输出目录
        temp_var     : 温度变量名（默认'tm'）
        threshold    : 高温阈值（默认35.0）
        min_duration : 最小持续时间（默认6）
        chunks       : Dask分块策略（默认时间分块100）
    """
    path = Path(nc_path)
    with xr.open_dataset(nc_path,chunks=chunks,engine="h5netcdf") as ds:
        if temp_var not in ds.data_vars:
            raise ValueError(f"Variable '{temp_var}' not found in dataset")
        with ProgressBar():
            tx90p = xr.apply_ufunc(
                _precentile,
                ds[temp_var],
                input_core_dims=[[time_dim]],
                kwargs={"percentile":threshold},
                vectorize=True,
                dask="parallelized",
                output_dtypes=[float]
            ).compute()
        tx90p.rio.to_raster(
            os.path.join(out_dir, f"{path.name.split('.')[0]}-TX90P.tif"),
            dtype="float32",
            compress="LZW"
        )
        del tx90p
    ds.close()
