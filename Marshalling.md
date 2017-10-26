In HW4 you are expected to write the marshall() code that inserts data into a relation and stores it to disk.

The data processor in insert_statement.py (line 50 and onward) expects a data datastream and a sometimes key datastream (as fields in the InsertRow class) that represent the data that will be written to disk.  Data has to be formatted in a particular way to be read correctly by the TableIterator when executing queries.

Hence, an inserted tuple must be marshalled into a format with a stream of int-values followed by a stream of bytes representing the non-key data. If a key exists, it is inserted separately.

For example, let's say that I want to execute:

 	insert into customer values (1, 'tim weninger', 'south bend', 'us', 450, 500);

Because cust_id is a primary key, I would expect that the key-stream would be: `['\x00\x00\x00\x01']` i.e., a 4-byte int with value 1. Offsets are never required for the key.

The data-stream would be: `['\xff\xff\xff\xff', '\x00\x00\x00\x1c', '\x00\x00\x00(', '\x00\x00\x002', '\x00\x00\x004', '\x00\x00\x008', '\x00\x00\x00<', 't', 'i', 'm', ' ', 'w', 'e', 'n', 'i', 'n', 'g', 'e', 'r', 's', 'o', 'u', 't', 'h', ' ', 'b', 'e', 'n', 'd', 'u', 's', '\x00\x00\x01\xc2', '\x00\x00\x01\xf4']`.

Let’s unpack this. We start with the offsets, which is 1 + the number of columns = 7. A primary key has an offset-code of -1, and a null value has an offset-code of -2 (as a code to the TableIterator). Since the first column is a primary key (and therefore not part of the datastream) we just enter an int of -1: ‘\xff\xff\xff\xff’.

The second column is actually in the datastream. Since there are seven offset-ints (where an int is 4 bytes) we know that it will start at 4*7 = 28 bytes in the datasteam. Hence the offset of the second column is `'\x00\x00\x00\x1c'` = 28.

Since the second column in the inserted data is 12 characters long, the next column will start at 28+12= `'\x00\x00\x00('` = 40. In this case my computer is interpreting the 40 as the ascii '(' instead of the hexadecimal. Your text-viewer may do the same thing – or maybe not.

The third column is 10 characters long, so the next offset will be 40+10 = `'\x00\x00\x002'` = 50. The '2'-character in this output is the ascii value for 50.

The fourth column is an 2 characters long, so the next offset will be 50+2 = `'\x00\x00\x004'` = 52. The '4'-character in the output is the ascii value for 52.

The fifth column is an int, which has a size of 4. So the next offset will be 52+4 = `'\x00\x00\x008'` = 56. The '8'-character is the ascii value for 56.

The final column is also an int, which has a size of 4. So the final offset will be 56+4 = `'\x00\x00\x00<'` = 60. The '<'-character is the ascii value for 60.

After the offsets, the data is streamed. You see that characters are printed as 1-byte characters, and ints as 4-byte ints. Make use of the functions within the DataOuputStream class to manipulate the streams.

The bottom-line is that whatever you write the stream (which gets output to disk) will need to be unmarshalled (i.e., read-in) by the TableIterator during a query.
