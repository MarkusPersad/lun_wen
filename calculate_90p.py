import xarray as xr
from dask.diagnostics.progress import ProgressBar
if __name__ == "__main__":
    with xr.open_dataset("ncs/1km/merged.nc",engine="h5netcdf",chunks={"valid_time": -1,"latitude": 10,"longitude": 10}) as ds:
        TX90p = ds['t2m'].quantile(0.9, dim="valid_time")
        with ProgressBar():
            TX90p.compute(num_workers=8)
        TX90p.rio.to_raster("ncs/1km/1km-90p.tif")
