import glob
import xarray as xr
import pandas as pd
import os
import pyogrio
import numpy as np
from datetime import datetime, timedelta
import rioxarray as rxr
from typing import List
from dask.diagnostics.progress import ProgressBar

def nc_to_tiff(nc_file:str,shapefile:str="",output_dir:str = "tiffs") -> None:
    """
    This function converts a NetCDF file to a TIFF file.
    TIFF影像的Value测量单位是0.1摄氏度
     参数：
         nc_file: 输入的NetCDF文件路径
         output_dir: 输出目录路径，默认"tiffs"
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with xr.open_dataset(nc_file) as ds:
        ds = ds.rename({"longitude": "lon", "latitude": "lat","valid_time":"time"})
        ds = ds.rio.write_crs("EPSG:4326")
        ds[list(ds.data_vars)[0]] = np.round((ds[list(ds.data_vars)[0]]-273.15)*10)
        if shapefile != "":
            gdf = pyogrio.read_dataframe(shapefile)
            gdf = gdf.to_crs(ds.rio.crs)
            geometry = gdf.geometry.unary_union #读取掩模文件
            ds = ds.rio.clip([geometry], drop=True) #按掩膜提取
        py_datetime = pd.to_datetime(ds.coords['time'].values)
        for time_idx, time in enumerate(py_datetime):
            daily_data = ds[list(ds.data_vars)[0]].isel(time=time_idx)
            output_dir_year = f"{output_dir}/{time.year}"
            if not os.path.exists(output_dir_year):
                os.makedirs(output_dir_year)
            daily_data.rio.to_raster(f"{output_dir_year}/{time.strftime('%Y%m%d')}.tif")

def rename_file(input_dir,output_dir=None)->None:
    if output_dir is None:
        output_dir = input_dir
    for file in os.listdir(input_dir):
        if not file.endswith(".tif"):
            continue
        original = file[:-4]
        year = original[:4]
        day_num = int(original[4:])
        start_date = datetime.strptime(f"{year}-01-01", "%Y-%m-%d")
        target_date = start_date + timedelta(days=day_num - 1)
        new_filename = f"{target_date.strftime('%Y%m%d')}.tif"
        new_filepath = os.path.join(output_dir, new_filename)
        os.rename(os.path.join(input_dir, file), new_filepath)


def tiffs_to_nc(input_dir:str,output_nc:str,var:str,chunks:int| dict =512,bands=False,works:int=4)->None:
    """
    将 TIFF 文件转换为 NetCDF 文件。
    参数：
        input_dir：输入文件夹路径。
        output_nc：输出 NetCDF 文件路径。
        var：变量名。
        chunks：块大小。
        bands：是否包含波段信息。
        works：工作线程数。
    """
    files = glob.glob(os.path.join(input_dir,"*.tif"))
    if not files:
        raise FileNotFoundError(f"未在文件夹 {input_dir} 中找到 TIFF 文件。")
    xds_array:List[xr.DataArray] = []
    for _,file in enumerate(files):
        date_str = os.path.basename(file).split('.')[0]
        try:
            date = pd.to_datetime(date_str,format="%Y%m%d")
        except ValueError:
            raise ValueError(f"无法解析日期字符串：{date_str}")
        xds = rxr.open_rasterio(file,chunks=chunks,cache=False)
        if isinstance(xds,xr.DataArray):
            xds = xds.astype(np.float32)
            xds = xds.assign_coords(time=date)
            if not bands:
                xds = xds.squeeze('band', drop=True)
            xds_array.append(xds)
        else:
            raise TypeError(f"无法处理数据类型：{type(xds)}")
    if not xds_array:
        raise ValueError("未找到有效的数据集或数据数组。")
    ds = xr.concat(xds_array,dim='time').to_dataset(name=var)
    ds =ds.rename({'y':'latitude','x':'longitude','time':'valid_time'})
    ds = ds.where(ds[var] != -9999, other=np.nan)
    # 设置变量和坐标的属性
    ds[var].attrs['standard_name'] = var
    ds[var].attrs['long_name'] = f"Daily Maximum Temperature ({var})"
    ds[var].attrs['units'] = "0.1°C"

    # 设置全局属性
    ds.attrs['title'] = "Daily Maximum Temperature Data"
    ds.attrs['institution'] = "Henan University"
    ds.attrs['source'] = "Zenodo.org"
    ds.attrs['history'] = f"Created on {datetime.now().strftime('%Y-%m-%d')}"
    if isinstance(chunks, dict):
        chunkSize = (1,chunks['y'],chunks['x'])
    elif isinstance(chunks,int):
        chunkSize = (1,chunks,chunks)
    encoding = {
            var: {
                'zlib': True,
                'complevel': 4,  # 平衡速度与压缩率
                'chunksizes': chunkSize,
                'shuffle': True,
            },
            'latitude': {'dtype': 'float32'},
            'longitude': {'dtype': 'float32'},
            'valid_time': {'dtype': 'float64'}
        }
    delayed = ds.to_netcdf(output_nc,encoding=encoding,engine='h5netcdf', compute=False)
    with ProgressBar():
        delayed.compute(num_workers=works)
