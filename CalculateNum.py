from lun_wen import calculate
import os

if __name__ == "__main__":
    input_dir = 'ncs/1km'
    output_dir = "calculated_data"
    output_dir = f"{output_dir}/{input_dir.split('/')[1]}"

    for file in os.listdir(input_dir):
        if file.endswith('.nc'):
            out = os.path.join(output_dir, file.split('.')[0])
            os.makedirs(out, exist_ok=True)
            # calculate.get_etccdi_stats(os.path.join(input_dir, file),var_name='t2m',output_dir=out,chunks={
            #     'valid_time': 100,
            #     'latitude': 71,
            #     'longitude': 122
            # })
            calculate.calculate_heatwave_events(os.path.join(input_dir,file),temp_var='t2m',threshold=350,out_dir=out,chunks={
                'valid_time': 100,
                'latitude': 1024,
                'longitude': 1024
            })
