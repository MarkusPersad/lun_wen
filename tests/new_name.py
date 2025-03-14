import os
import numpy as np
import rasterio
import pymannkendall as mk
from glob import glob

# 数据文件夹路径
data_dir = "data/ssi_s"

# 结果保存路径
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)  # 确保输出目录存在

# 获取所有.tif文件路径
file_paths = sorted(glob(os.path.join(data_dir, "*.tif.tif")))
print(f"找到 {len(file_paths)} 个 SSI 文件")  # 调试信息

# **修正年份提取**
years = sorted(set(os.path.basename(fp).split("_")[1] for fp in file_paths))
print(f"提取到 {len(years)} 个年份: {years}")  # 调试信息，确保是 2003, 2004, ..., 2023

# 计算年均SSI
yearly_means = {}
for year in years:
    yearly_files = sorted(glob(os.path.join(data_dir, f"SSI_{year}_*.tif.tif")))
    if yearly_files:
        ssi_stack = []
        with rasterio.open(yearly_files[0]) as src:
            meta = src.meta.copy()
            meta.update(dtype=rasterio.float32, count=1)
        for f in yearly_files:
            with rasterio.open(f) as src:
                ssi_stack.append(src.read(1))  # 读取数据
        yearly_means[year] = np.nanmean(ssi_stack, axis=0)  # 计算年均值

print(f"yearly_means 共有 {len(yearly_means)} 年数据")  # 调试信息

# **检查 year_means 是否为空**
if not yearly_means:
    raise ValueError("错误: 没有计算出任何年均SSI数据，请检查输入文件！")

# 转换为数组
years_sorted = sorted(yearly_means.keys())  # 确保按年份顺序
ssi_array = np.array([yearly_means[yr] for yr in years_sorted])  # 形成 (年, 行, 列) 结构

print(f"ssi_array 维度: {ssi_array.shape}")  # 调试信息

# **检查 ssi_array 是否为空**
if ssi_array.size == 0:
    raise ValueError("错误: ssi_array 为空，无法进行趋势分析！")

# 计算Sen斜率与Mann-Kendall检验
sen_slope = np.zeros(ssi_array.shape[1:], dtype=np.float32)
mk_trend = np.zeros(ssi_array.shape[1:], dtype=np.float32)

rows, cols = ssi_array.shape[1:]  # 获取行列数
for i in range(rows):
    for j in range(cols):
        pixel_series = ssi_array[:, i, j]
        if np.isnan(pixel_series).all():  # 跳过全是NaN的像素
            sen_slope[i, j] = np.nan
            mk_trend[i, j] = np.nan
        else:
            trend_result = mk.original_test(pixel_series)
            sen_slope[i, j] = trend_result.slope
            mk_trend[i, j] = trend_result.z  # MK检验的Z值，正值表示上升趋势

# 结果保存路径
output_sen = os.path.join(output_dir, "SSI_Sen_Slope.tif")
output_mk = os.path.join(output_dir, "SSI_MK_Trend.tif")

# 保存计算结果
for data, output_path in zip([sen_slope, mk_trend], [output_sen, output_mk]):
    with rasterio.open(output_path, 'w', **meta) as dst:
        dst.write(data, 1)

print(f"计算完成，结果已保存至:\n{output_sen}\n{output_mk}")
#
# from lun_wen import mannkendall
# import xarray as xr
# from dask.diagnostics.progress import ProgressBar
# if __name__ == "__main__":
#     mannkendall.mannkendall(nc_path="data/ssi_year.nc",
#         out_dir="output",
#         var_name="t2m",
#         time_dim="valid_time",
#         chunks={"valid_time": -1, "latitude": 256, "longitude": 512}
#     )
#     with xr.open_dataset("data/ssi.nc",engine="h5netcdf",chunks={"valid_time": -1, "latitude": 256, "longitude": 512}) as ds :
#         with ProgressBar():
#             ds = ds.resample(valid_time="YE").mean().compute()
#             ds.to_netcdf("output/ssi_year.nc")
#             ds.close()
