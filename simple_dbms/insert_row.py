from data_output_stream import DataOutputStream
from column import Column


class InsertRow:
    # Constants for special offsets
    # The field with this offset is a primary key.
	IS_PKEY = -1

    # The field with this offset has a null value.
	IS_NULL = -2

	def __init__(self, table, values):
		"""
		Constructs an InsertRow object for a row containing the specified
		values that is to be inserted in the specified table.
		:param table:
		:param values:
		"""
		self._table = table
		self._values = values

        # These objects will be created by the marshall() method.
		self._key = None
		self._data = None

	def marshall(self):
		"""
		Takes the collection of values for this InsertRow
		and marshalls them into a key/data pair.
		:return:
		"""

		offset = 4 * (self._table.num_columns() + 1)
		colNum = 0
		self._data = DataOutputStream()	
		
		for val in self._values:

			if val is None:
				offset_code = -2
				self._data.write_int(offset_code)

			col = self._table.get_column(colNum)
			if col.is_primary_key():
				offset_code = -1
				print 'key offset'
				self._data.write_int(offset_code)
				
			if type(val) is int:
				offset = offset + 4
				print 'int offset'
				self._data.write_int(offset)				

			if type(val) is str:
				offset = offset + len(val)
				print 'str offset'
				self._data.write_int(offset)

			if type(val) is float:
				offset = offset + 8
				print 'float offset'
				self._data.write_int(offset)

			colNum += 1

		colNum = 0

		for val in self._values:

			if val is None:
				continue

			col = self._table.get_column(colNum)
			if col.is_primary_key():
				print 'insert key'
				if type(val) is int:
					print 'int'
					self._data.write_int(val)
				elif type(val) is str:
					print 'str'
					val = bytearray(val)
					for c in val:
						self._data.write_byte(c)
				elif type(val) is float:
					print 'float'
					self._data.write_float(val)
				print 'inserted key'

			if type(val) is int:
				print 'insert int'
				self._data.write_int(val)				
				print 'inserted int'

			if type(val) is str:
				print 'insert str'
				val = bytearray(val)
				for c in val:
					self._data.write_byte(c)

			if type(val) is float:
				print 'insert float'
				self._data.write_float(val)



	def get_key(self):
		"""
		Returns the key in the key/data pair for this row.
		:return: the key
		"""
		return self._key

	def get_data(self):
		"""
		Returns the data item in the key/data pair for this row.
		:return: the data
		"""
		return self._data
