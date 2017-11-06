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

			col = self._table.get_column(colNum)
			if col.is_primary_key():
				offset_code = -1
				self._key = DataOutputStream()
				self._data.write_int(offset_code)

			elif val is None:
				offset_code = -2
				self._data.write_int(offset_code)
	
			elif type(val) is int:
				self._data.write_int(offset)
				offset = offset + 4				

			elif type(val) is str:
				self._data.write_int(offset)
				offset = offset + len(val)

			elif type(val) is float:
				self._data.write_int(offset)
				offset = offset + 8

			colNum += 1

		colNum = 0

		for val in self._values:

			if val is None:
				continue

			col = self._table.get_column(colNum)
			if col.is_primary_key():
				if type(val) is int:
					self._data.write_int(val)
				elif type(val) is str:
					val = bytearray(val)
					for c in val:
						self._data.write_byte(c)
				elif type(val) is float:
					self._data.write_float(val)
				self._key.write_int(val)

			elif type(val) is int:
				self._data.write_int(val)				

			elif type(val) is str:
				val = bytearray(val)
				for c in val:
					self._data.write_byte(c)

			elif type(val) is float:
				self._data.write_float(val)
		
			colNum += 1


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
