from typing import Optional, Tuple
import os
import shutil

def copy_files_by_keyword(
    source_dir: str,
    dest_dir: str,
    suffix: Optional[str] = None,
    key_words: list[str] = []
) -> Tuple[int, int]:
    total_matched = 0
    success_count = 0
    os.makedirs(dest_dir, exist_ok=True)

    # 去重关键字并创建子目录
    unique_keywords = list(set(key_words))
    for kw in unique_keywords:
        os.makedirs(os.path.join(dest_dir, kw), exist_ok=True)

    for root, _, files in os.walk(source_dir):
        if suffix:
            files = [f for f in files if f.endswith(suffix)]

        for file in files:
            # 获取第一个匹配的关键字（避免多目录重复）
            matched_keywords = [kw for kw in unique_keywords if kw in file]
            if not matched_keywords:
                continue

            total_matched += 1
            src_path = os.path.join(root, file)

            try:
                # 只复制到第一个匹配目录（按关键字列表顺序）
                keyword = matched_keywords[0]
                dest_subdir = os.path.join(dest_dir, keyword)
                base_name, ext = os.path.splitext(file)
                dest_path = os.path.join(dest_subdir, file)

                # 处理重名冲突
                counter = 1
                while os.path.exists(dest_path):
                    new_name = f"{base_name}_{counter}{ext}"
                    dest_path = os.path.join(dest_subdir, new_name)
                    counter += 1

                shutil.copy2(src_path, dest_path)
                success_count += 1
                print(f"✓ 已复制: {file} → {os.path.relpath(dest_path, dest_dir)}")

            except (FileNotFoundError, PermissionError) as e:
                print(f"× 错误 {type(e).__name__}: {src_path}")
            except Exception as e:
                print(f"× 未知错误 {type(e).__name__}: {src_path}")

    print(f"\n操作完成: 共匹配{total_matched}个文件，成功复制{success_count}个")
    return total_matched, success_count
