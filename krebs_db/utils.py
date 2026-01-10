# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.


from sqlalchemy import Enum

def db_enum(enum_cls):
    return Enum(enum_cls, native_enum=False, values_callable=lambda e: [i.value for i in e])