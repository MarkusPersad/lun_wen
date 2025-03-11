from dask.diagnostics.progress import ProgressBar
import xarray as xr
import glob


if __name__ == "__main__":
    with xr.open_mfdataset(glob.glob("ncs/1km/*.nc"),engine="h5netcdf",concat_dim="valid_time",combine="nested",chunks={'valid_time': 100,"latitude":1024,"longitude":1024}) as ds:
        encoding = {
                "t2m": {
                    'zlib': True,
                    'complevel': 4,  # 平衡速度与压缩率
                    'chunksizes': (100, 1024,1024),
                    'shuffle': True,
                },
                'latitude': {'dtype': 'float32'},
                'longitude': {'dtype': 'float32'},
                'valid_time': {'dtype': 'float64'}
            }
        delayed = ds.to_netcdf("ncs/1km/merged.nc",encoding=encoding,engine='h5netcdf', compute=False)
        with ProgressBar():
            delayed.compute(num_workers=8)
            TX90p = ds["t2m"].chunk({'valid_time': 1000,"latitude":"auto","longitude":"auto"}).quantile(0.9, dim="valid_time").compute()
            TX90p.rio.to_raster("ncs/0.25/0.25-90p.tif")
