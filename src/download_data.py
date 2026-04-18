"""
Script for downloading NYC Yellow Taxi data.
Downloading Parquet files of NYC TLC for given months.
"""

import os
import sys
from pathlib import Path
import requests
import yaml

def load_config(config_path: str = "config/config.yaml") -> dict:
    """Loading the Yaml config"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    
def download_file(url: str, destination: Path) -> bool:
    """
    Downloads one file from URL.
    Return True for sucess and False for error.
    """
    if destination.exists():
        size_mb =  destination.stat().st_size / (1024 * 1024)
        print(f"File is there: {destination.name} ({size_mb:.1f} MB)")
        return True
    
    try:
        print(f" Downloads: {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                # Progress of each 5 MB
                if downloaded % (5 * 1024 * 1024) < 8192:
                    mb = downloaded / (1024 * 1024)
                    if total_size > 0:
                        pct = (downloaded / total_size) * 100
                        print(f"    {mb:.1f} MB ({pct:.0f}%)", end="\r")

        size_mb = destination.stat().st_size / (1024 * 1024)
        print(f"  ✓ Downloaded: {destination.name} ({size_mb:.1f} MB)")
        return True

    except requests.RequestException as e:
        print(f"  ✗ Error while downloading: {e}")
        if destination.exists():
            destination.unlink()  # Delete file
        return False    

def main():
    """Main Function - donwloads all file from the config"""
    config = load_config()

    base_url = config["data_source"]["base_url"]
    taxi_type = config["data_source"]["taxi_type"]
    months = config["data_source"]["months"]
    raw_dir = Path(config["paths"]["raw_data"])

    # Създай папката ако не съществува
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Download of {taxi_type} taxi data")
    print(f"{'='*60}")
    print(f"Folder: {raw_dir.absolute()}")
    print(f"Months for download: {months}\n")

    success_count = 0
    for month in months:
        filename = f"{taxi_type}_tripdata_{month}.parquet"
        url = f"{base_url}/{filename}"
        destination = raw_dir / filename

        print(f"Month {month}:")
        if download_file(url, destination):
            success_count += 1
        print()

    print(f"{'='*60}")
    print(f"Ready: {success_count}/{len(months)} files downloaded")
    print(f"{'='*60}\n")

    return 0 if success_count == len(months) else 1


if __name__ == "__main__":
    sys.exit(main())