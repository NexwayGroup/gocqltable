package gocqltable

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/gocql/gocql"
	"encoding/json"
)

type Counter int

func stringTypeOf(i interface{}) (string, error) {
	_, isByteSlice := i.([]byte)
	if !isByteSlice {
		// Check if we found a higher kinded type
		switch reflect.ValueOf(i).Kind() {
		case reflect.Slice:
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			ct := cassaType(elemVal)
			if ct == gocql.TypeCustom {
				// the typeCustom will be converted and stored as a json string so here it will be a list of json strings
				ct = gocql.TypeVarchar
			}
			return fmt.Sprintf("list<%v>", ct), nil
		case reflect.Map:
			keyVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Key())).Interface()
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			keyCt := cassaType(keyVal)
			elemCt := cassaType(elemVal)
			if keyCt == gocql.TypeCustom || elemCt == gocql.TypeCustom {
				return "", fmt.Errorf("Unsupported map key or value type %T", i)
			}
			return fmt.Sprintf("map<%v, %v>", keyCt, elemCt), nil
		}
	}
	ct := cassaType(i)
	if ct == gocql.TypeCustom {
		// the typeCustom will be converted and stored as a json string
		ct = gocql.TypeVarchar
	}
	return cassaTypeToString(ct)
}

// ProcessValue take the input parameter and convert it to a json string or a slice of json strings
func ProcessValue(i interface{}) (interface{}, error) {
	if i == nil {
		return nil, nil
	}
	_, isByteSlice := i.([]byte)
	if !isByteSlice {
		// Check if we found a higher kinded type
		switch reflect.ValueOf(i).Kind() {
		case reflect.Slice:
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			ct := cassaType(elemVal)
			if ct == gocql.TypeCustom {
				s := reflect.ValueOf(i)
				result := make([]string, 0, s.Len())
				for j := 0; j < s.Len(); j++ {
					txt, err := json.Marshal(s.Index(j).Interface())
					if err != nil {
						return "", fmt.Errorf("Unsupported translation of value type %T", err)
					}
					result = append(result, string(txt))
				}
				return result, nil
			}
			return i, nil
		case reflect.Map:
			keyVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Key())).Interface()
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			keyCt := cassaType(keyVal)
			elemCt := cassaType(elemVal)
			if keyCt == gocql.TypeCustom || elemCt == gocql.TypeCustom {
				return "", fmt.Errorf("Unsupported map key or value type %T", i)
			}
			return i, nil
		}
	}
	ct := cassaType(i)
	if ct == gocql.TypeCustom {
		res, err := json.Marshal(i)
		return string(res), err
	}
	return i, nil
}

func cassaType(i interface{}) gocql.Type {
	switch i.(type) {
	case int, int32:
		return gocql.TypeInt
	case int64:
		return gocql.TypeBigInt
	case string:
		return gocql.TypeVarchar
	case float32:
		return gocql.TypeFloat
	case float64:
		return gocql.TypeDouble
	case bool:
		return gocql.TypeBoolean
	case time.Time:
		return gocql.TypeTimestamp
	case gocql.UUID:
		return gocql.TypeUUID
	case []byte:
		return gocql.TypeBlob
	case Counter:
		return gocql.TypeCounter
	}
	// if the type is unknown, try to match its kind before declaring it as custom
	switch reflect.ValueOf(i).Kind() {
	case reflect.Int, reflect.Int32:
		return gocql.TypeInt
	case reflect.Int64:
		return gocql.TypeBigInt
	case reflect.String:
		return gocql.TypeVarchar
	case reflect.Float32:
		return gocql.TypeFloat
	case reflect.Float64:
		return gocql.TypeDouble
	case reflect.Bool:
		return gocql.TypeBoolean
	}
	return gocql.TypeCustom
}

func cassaTypeToString(t gocql.Type) (string, error) {
	switch t {
	case gocql.TypeInt:
		return "int", nil
	case gocql.TypeBigInt:
		return "bigint", nil
	case gocql.TypeVarchar:
		return "varchar", nil
	case gocql.TypeFloat:
		return "float", nil
	case gocql.TypeDouble:
		return "double", nil
	case gocql.TypeBoolean:
		return "boolean", nil
	case gocql.TypeTimestamp:
		return "timestamp", nil
	case gocql.TypeUUID:
		return "uuid", nil
	case gocql.TypeBlob:
		return "blob", nil
	case gocql.TypeCounter:
		return "counter", nil
	default:
		return "", errors.New("unkown cassandra type")
	}
}
