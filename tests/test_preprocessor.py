import pytest
from ghcn_preprocess import GHCNPreprocessor
from pathlib import Path

def test_preprocessor_initialization():
    processor = GHCNPreprocessor(
        folder="data/",
        out_csv="output.csv",
        min_lat=30, max_lat=38,
        min_lon=-116, max_lon=-107
    )
    assert isinstance(processor, GHCNPreprocessor)
    assert isinstance(processor.folder, Path)