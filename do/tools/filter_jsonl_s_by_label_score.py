import json
import os
from pathlib import Path
import argparse

def filter_jsonl_by_mold_score(input_file, output_file, threshold=3.0):
    """
    Filter JSONL file entries where mold-score is less than the threshold.
    
    Args:
        input_file (str): Path to input JSONL file
        output_file (str): Path to output JSONL file
        threshold (float): Threshold for mold-score filtering
    """
    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', encoding='utf-8') as f_out:
        for line in f_in:
            try:
                data = json.loads(line.strip())
                # if data.get('mold-score', float('inf')) < threshold:
                if data.get('mold-score', float('inf')) >= threshold:
                    f_out.write(json.dumps(data, ensure_ascii=False) + '\n')
            except json.JSONDecodeError:
                print(f"Warning: Skipping invalid JSON line in {input_file}")
                continue

def process_directory(input_dir, output_dir=None, threshold=3.0):
    """
    Process all JSONL files in the input directory.
    
    Args:
        input_dir (str): Path to directory containing JSONL files
        output_dir (str, optional): Path to output directory. If None, will create a new directory with "_filtered" suffix
        threshold (float, optional): Threshold for mold-score filtering. Defaults to 3.0
    """
    input_path = Path(input_dir)
    
    # Create output directory if not specified
    if output_dir is None:
        output_dir = input_path.parent / f"{input_path.name}_filtered"
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(exist_ok=True)
    
    # Process each JSONL file
    for jsonl_file in input_path.glob('*.jsonl'):
        output_file = output_dir / f"{jsonl_file.stem}_45{jsonl_file.suffix}"           # 输出文件名_012是小于3
        print(f"Processing {jsonl_file.name} -> {output_file.name}")
        filter_jsonl_by_mold_score(str(jsonl_file), str(output_file), threshold)


if __name__ == "__main__":
    input_dir = r"D:\Projects\data\fasttext_train\mold_markdown"
    output_dir = r"D:\Projects\data\fasttext_train\mold_markdown\45"
    # output_dir = r"D:\Projects\data\fasttext_train\mold_more_than_4"
    threshold = 4.0
    process_directory(input_dir, output_dir, threshold)
    print("Processing completed!")