# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

from sqlalchemy import Enum
import base64
import io


def db_enum(enum_cls):
    return Enum(enum_cls, native_enum=False, values_callable=lambda e: [i.value for i in e])


def base64_to_file(value: str) -> io.BytesIO:
    """
    Decodes a base64-encoded string and returns a file-like object.
    """
    decoded_bytes = base64.b64decode(value)
    file_like_object = io.BytesIO(decoded_bytes)
    return file_like_object
