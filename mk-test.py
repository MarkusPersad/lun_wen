from lun_wen import mannkendall
import os


if __name__ == "__main__":
    input_dir = "mk/0.25"
    for file in os.listdir(input_dir):
        if not file.endswith(".nc"):
            continue
        mannkendall.mannkendall(os.path.join(input_dir, file),chunks={"valid_time":-1,"latitude":1024,"longitude":1024},var_name="t2m",time_dim="valid_time")
