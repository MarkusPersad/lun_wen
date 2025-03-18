import xarray as xr
import numpy as np
from dask.diagnostics.progress import ProgressBar
import os
from numba import jit
# import pymannkendall as mk

@jit(nopython=True)
def _sens_slope(x: np.ndarray) -> float:
    """计算Sen's斜率（优化版）"""
    n = len(x)
    slopes = []
    for i in range(n-1):
        for j in range(i+1, n):
            dx = j - i
            dy = x[j] - x[i]
            if dx != 0:
                slopes.append(dy / dx)
    if len(slopes) == 0:
        return np.nan
    return float(np.median(np.array(slopes)))

@jit(nopython=True)
def _mk_score(x: np.ndarray) -> int:
    """计算Mann-Kendall S统计量"""
    s = 0
    n = len(x)
    for k in range(n-1):
        for j in range(k+1, n):
            if x[j] > x[k]:
                s += 1
            elif x[j] < x[k]:
                s -= 1
    return s

@jit(nopython=True)
def _variance_s(x: np.ndarray) -> float:
    """计算方差（兼容ties处理）"""
    n = len(x)
    if n < 2:
        return 0.0

    # 手动统计ties
    sorted_x = np.sort(x)
    tie_counts = []
    current_count = 1

    for i in range(1, n):
        if sorted_x[i] == sorted_x[i-1]:
            current_count += 1
        else:
            if current_count > 1:
                tie_counts.append(current_count)
            current_count = 1
    if current_count > 1:
        tie_counts.append(current_count)

    # 计算方差
    tie_sum = 0.0
    for c in tie_counts:
        tie_sum += c * (c-1) * (2*c +5)

    return (n*(n-1)*(2*n+5) - tie_sum) / 18.0

@jit(nopython=True)
def _mk_z(s: int, var_s: float) -> float:
    """计算Z值（修正参数顺序）"""
    if s > 0:
        return (s - 1) / np.sqrt(var_s)
    elif s < 0:
        return (s + 1) / np.sqrt(var_s)
    else:
        return 0.0
def mannkendall(
            nc_path: str,
            time_dim: str = "time",  # 增加时间维度参数
            chunks: dict = {"latitude": 100, "longitude": 100},
            var_name: str = 't2m',
            out_dir: str = "./output",
            trusted: float = 1.96
        ):
            # 创建输出目录
            os.makedirs(out_dir, exist_ok=True)

            # 数据加载
            with xr.open_dataset(nc_path, chunks=chunks) as ds:
                # 维度校验
                if time_dim not in ds[var_name].dims:
                    raise ValueError(f"时间维度 {time_dim} 不存在于变量 {var_name}")

                # 定义计算函数
                def _calc_slope(arr: np.ndarray) -> float:
                    valid = arr[~np.isnan(arr)]
                    if len(valid) < 2:
                        return np.nan
                    return _sens_slope(valid)
                    # return mk.sens_slope(valid).slope

                def _calc_z(arr: np.ndarray) -> float:
                    valid = arr[~np.isnan(arr)]
                    if len(valid) < 2:
                        return np.nan
                    s = _mk_score(valid)
                    var_s = _variance_s(valid)
                    return _mk_z(s, var_s)
                    # return mk.original_test(valid).z

                # 并行计算
                with ProgressBar():
                    sen_slope = xr.apply_ufunc(
                        _calc_slope,
                        ds[var_name],
                        input_core_dims=[[time_dim]],
                        dask='parallelized',
                        vectorize=True,
                        output_dtypes=[float]
                    ).compute()

                    z_score = xr.apply_ufunc(
                        _calc_z,
                        ds[var_name],
                        input_core_dims=[[time_dim]],
                        dask='parallelized',
                        vectorize=True,
                        output_dtypes=[float]
                    ).compute()

                # 结果处理
                significant = sen_slope.where(np.abs(z_score) >= trusted)

                result = xr.Dataset({
                    "Slope":sen_slope,
                    "Z-score":z_score,
                    "Significant":significant
                })
                base = os.path.basename(nc_path).split('.')[0]
                result.to_netcdf(os.path.join(out_dir, f"{base}_Slope_MK.nc"))

                # 保存结果
                base = os.path.basename(nc_path).split('.')[0]
                sen_slope.rio.to_raster(
                    os.path.join(out_dir, f"{base}_sen.tif"),
                    dtype='float32',
                    compress='LZW'
                )
                z_score.rio.to_raster(
                    os.path.join(out_dir, f"{base}_mk_z.tif"),
                    dtype='float32',
                    compress='LZW'
                )
                significant.rio.to_raster(
                    os.path.join(out_dir, f"{base}_significant.tif"),
                    dtype='float32',
                    compress='LZW'
                )

                print(f"处理完成！结果保存至：{out_dir}")
