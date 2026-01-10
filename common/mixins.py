# Licensed under the MIT License.
# Copyright (c) 2025 Nataliya Didukh, Ihor Zhvanko.
# See the LICENSE file in the project root for full license text.

class ReprMixin:
    def __repr__(self):
        values = []
        for key in self.__mapper__.columns.keys():
            value = getattr(self, key, None)
            values.append(f"{key}={value!r}")

        for relationship in self.__mapper__.relationships:
            value = getattr(self, relationship.key)
            values.append(f"\n\t{relationship.key}={value!r}")

        return f"<{self.__class__.__name__}({', '.join(values)})>"