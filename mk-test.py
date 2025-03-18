from lun_wen import mannkendall
import os


if __name__ == "__main__":
    input_dir = "mk/year/1km"
    for file in os.listdir(input_dir):
        if not file.endswith(".nc"):
            continue
        mannkendall.mannkendall(
            nc_path=os.path.join(input_dir, file),
            chunks={"valid_time":-1,"latitude":1024,"longitude":1024},
            var_name="t2m",
            time_dim="valid_time",
            out_dir="mk/result/year/1km")
