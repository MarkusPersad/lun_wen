from pathlib import Path
from lun_wen.translate_data import tiffs_to_nc

if __name__ == "__main__":
    # 定义输入输出路径
    input_dir = Path("0.25")
    output_dir = Path("ncs")

    # 创建输出父目录（自动处理路径存在性）
    output_parent = output_dir / input_dir.name
    output_parent.mkdir(parents=True, exist_ok=True)

    # 获取所有子文件夹（仅目录）
    folders = [folder for folder in input_dir.glob("*") if folder.is_dir()]

    # 遍历处理每个子文件夹
    for folder in folders:
        # 构建输出文件路径
        output_nc = output_parent / f"{folder.name}.nc"

        # 调用转换函数
        tiffs_to_nc(
            input_dir=str(folder),  # 直接使用子文件夹的完整路径
            output_nc=str(output_nc),
            var="t2m",
            chunks={
                'y': 71,
                'x': 122
            }
        )
