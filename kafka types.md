Type	Description

BOOLEAN	Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true respectively. When reading a boolean value, any non-zero value is considered true.

INT8	Represents an integer between -27 and 27-1 inclusive.

INT16	Represents an integer between -215 and 215-1 inclusive. The values are encoded using two bytes in network byte order (big-endian).

INT32	Represents an integer between -231 and 231-1 inclusive. The values are encoded using four bytes in network byte order (big-endian).

INT64	Represents an integer between -263 and 263-1 inclusive. The values are encoded using eight bytes in network byte order (big-endian).

UINT32	Represents an integer between 0 and 232-1 inclusive. The values are encoded using four bytes in network byte order (big-endian).

VARINT	Represents an integer between -231 and 231-1 inclusive. Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.

VARLONG	Represents an integer between -263 and 263-1 inclusive. Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.

UUID	Represents a type 4 immutable universally unique identifier (Uuid). The values are encoded using sixteen bytes in network byte order (big-endian).

FLOAT64	Represents a double-precision 64-bit format IEEE 754 value. The values are encoded using eight bytes in network byte order (big-endian).

STRING	Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. Length must not be negative.

COMPACT_STRING	Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT . Then N bytes follow which are the UTF-8 encoding of the character sequence.

NULLABLE_STRING	Represents a sequence of characters or null. For non-null strings, first the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is encoded with length of -1 and there are no following bytes.

COMPACT_NULLABLE_STRING	Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT . Then N bytes follow which are the UTF-8 encoding of the character sequence. A null string is represented with a length of 0.

BYTES	Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.

COMPACT_BYTES	Represents a raw sequence of bytes. First the length N+1 is given as an UNSIGNED_VARINT.Then N bytes follow.

NULLABLE_BYTES	Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following bytes.


COMPACT_NULLABLE_BYTES	Represents a raw sequence of bytes. First the length N+1 is given as an UNSIGNED_VARINT.Then N bytes follow. A null object is represented with a length of 0.

RECORDS	Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see Message Sets.

ARRAY	Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure. First, the length N is given as an INT32. Then N instances of type T follow. A null array is represented with a length of -1. In protocol documentation an array of T instances is referred to as [T].

COMPACT_ARRAY	Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure. First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow. A null array is represented with a length of 0. In protocol documentation an array of T instances is referred to as [T].