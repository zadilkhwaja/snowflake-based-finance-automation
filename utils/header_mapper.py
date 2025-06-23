import re
import pandas as pd
from config.schema_config import TARGET_SCHEMAS, HEADER_SYNONYMS


def normalize_header(header):
    header = header.strip().lower()
    header = re.sub(r'[^a-z0-9]', '_', header)
    header = re.sub(r'_+', '_', header)
    return header.strip('_')


def build_reverse_mapping(synonyms):
    reverse_map = {}
    for target_col, variations in synonyms.items():
        for variant in variations:
            reverse_map[normalize_header(variant)] = target_col
        reverse_map[normalize_header(target_col)] = target_col
    return reverse_map


REVERSE_HEADER_MAP = build_reverse_mapping(HEADER_SYNONYMS)


def map_headers(source_headers, doc_type):
    target_schema = TARGET_SCHEMAS[doc_type]
    normalized_source = {normalize_header(h): h for h in source_headers}

    mapped = {}
    for target_col in target_schema:
        matched_col = None
        for norm_header in normalized_source:
            if REVERSE_HEADER_MAP.get(norm_header) == target_col:
                matched_col = normalized_source[norm_header]
                break
        mapped[target_col] = matched_col
    return mapped


def map_dataframe_to_schema(df, doc_type):
    mapping = map_headers(df.columns, doc_type)
    output_data = {}
    for target_col in TARGET_SCHEMAS[doc_type]:
        source_col = mapping.get(target_col)
        if source_col and source_col in df:
            output_data[target_col] = df[source_col]
        else:
            output_data[target_col] = ""
    return pd.DataFrame(output_data)
