
from lun_wen.calculate import tx90p_count


if __name__ == "__main__":
    tx90p_count(
        nc_path="ncs/1km/2003.nc",
        out_dir="output",
        temp_var="t2m",
        threshold=90,
        chunks={
            "valid_time": -1,
            "latitude": 1024,
            "longitude": 1024
        }
    )
