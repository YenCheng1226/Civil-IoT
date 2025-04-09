import os
import requests
import zipfile
import base64

from tqdm import tqdm
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed


class FileFetcher:
    def __init__(self, api_url, base_url, headers, download_dir, extract_dir):
        self.api_url = api_url
        self.base_url = base_url
        self.headers = headers
        self.download_dir = download_dir
        self.extract_dir = extract_dir
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(extract_dir, exist_ok=True)

    def fetch_zip_links(self):
        
        response = requests.post(self.api_url, headers=self.headers, json={})
        response.raise_for_status()

        try:
            data = response.json()

        except Exception as e:
            print(response.text[:500])
            response.raise_for_status() 
            raise

        return [
            f["path"]
            for f in data["data"]["files"]
            if f["type"] == "file" and f["name"].endswith(".zip")
        ]

    def _download_single(self, file_path):
        file_name = os.path.basename(file_path)
        encoded_path = quote(base64.b64encode(file_path.encode()).decode(), safe="")
        file_url = f"{self.base_url}/?r=/download&path={encoded_path}"
        save_path = os.path.join(self.download_dir, file_name)

        try:
            print(f"Downloading: {file_name}")
            response = requests.get(file_url, headers=self.headers, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            with open(save_path, "wb") as file, tqdm(
                desc=file_name,
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                leave=False,
            ) as bar:
                for data in response.iter_content(chunk_size=1024):
                    file.write(data)
                    bar.update(len(data))

            with open(save_path, "rb") as f:
                if f.read(4) != b"PK\x03\x04":
                    print(f"{file_name} is not a valid ZIP file")
                    return

            with zipfile.ZipFile(save_path, "r") as zip_ref:
                zip_ref.extractall(self.extract_dir)

            print(f"{file_name} extracted to {self.extract_dir}")

        except Exception as e:
            print(f"Failed to download/extract {file_name}: {e}")

    def download_and_extract(self, file_paths, max_workers=5):
        if isinstance(file_paths, str):
            self._download_single(file_paths)
        elif isinstance(file_paths, list):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self._download_single, path) for path in file_paths]
                for future in as_completed(futures):
                    future.result()
        else:
            raise ValueError("file_paths: string or list[str]")
