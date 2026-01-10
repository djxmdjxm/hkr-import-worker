
from sqlalchemy.orm import Mapper

class ReprMixin:
    def __repr__(self):
        values = []
        for key in self.__mapper__.columns.keys():
            value = getattr(self, key, None)
            values.append(f"{key}={value!r}")

        for relationship in self.__mapper__.relationships:
            value = getattr(self, relationship.key)
            values.append(f"\n\t{relationship.key}={value!r}")

#            try:
                
                #print('>>>>> ', relationship.key, value)
                # Show only related object's ID or repr (avoid recursion)
            #     if value is None:
            #         fields[rel.key] = None
            #     elif rel.uselist:
            #         fields[rel.key] = [getattr(obj, 'id', repr(obj)) for obj in value]
            #     else:
            #         fields[rel.key] = getattr(value, 'id', repr(value))
            # except Exception as e:
            #     fields[rel.key] = f"<error: {e}>"


        return f"<{self.__class__.__name__}({', '.join(values)})>"