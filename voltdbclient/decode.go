/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package voltdbclient

import (
	"fmt"
	"reflect"
)

// Internal methods to unmarshal / reflect a returned table []byte
// into a slice of user provided row structs.

func (table *Table) next(v interface{}) error {
	// iterate and assign the fields from data
	// must have a pointer to be modifiable
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("Must supply a struct pointer")
	}

	// must have a struct
	structVal := rv.Elem()
	typeOfT := structVal.Type()
	if typeOfT.Kind() != reflect.Struct {
		return fmt.Errorf("Must supply a struct to populate with row data.")
	}

	if structVal.NumField() != int(table.columnCount) {
		return fmt.Errorf("Must supply one field per column.")
	}

	// stupid alias to type a bit less...
	r := &table.rows

	// each row has a 4 byte length
	rowLength, err := readInt(r)
	if err != nil {
		return err
	} else if rowLength <= 0 {
		return fmt.Errorf("No more row data.")
	}

	for idx, vt := range table.columnTypes {
		structField := structVal.Field(idx)
		switch vt {
		case vt_BOOL:
			val, _ := readBoolean(r)
			structField.SetBool(val)
		case vt_SHORT:
			val, _ := readShort(r)
			structField.SetInt(int64(val))
		case vt_INT:
			val, _ := readInt(r)
			structField.SetInt(int64(val))
		case vt_LONG:
			val, _ := readLong(r)
			structField.SetInt(val)
		case vt_FLOAT:
			val, _ := readFloat(r)
			structField.SetFloat(val)
		case vt_STRING:
			val, _ := readString(r)
			structField.SetString(val)
		case vt_TIMESTAMP:
			val, _ := readTimestamp(r)
			structField.Set(reflect.ValueOf(val))
		case vt_TABLE:
			panic("Can not deserialize embedded tables.")
		case vt_DECIMAL:
			panic("Can not deserialize decimals yet.")
		case vt_VARBIN:
			panic("Can not deserialize varbinary yet.")
		default:
			panic("Unknown type in deserialize type")
		}
	}

	return nil
}
