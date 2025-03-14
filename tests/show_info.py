import xarray as xr

if __name__ == "__main__":
    ds = xr.open_dataset('data/ssi.nc')
    print(ds['valid_time'])
    ds.close()
