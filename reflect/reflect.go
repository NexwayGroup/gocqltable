// This package provides some punk-rock reflection which is not in the stdlib.
package reflect

import (
	"fmt"
	r "reflect"
	"strings"
	"sync"
	"encoding/json"
)

// StructToMap converts a struct to map. The object's default key string
// is the struct field name but can be specified in the struct field's
// tag value. The "cql" key in the struct field's tag value is the key
// name. Examples:
//
//   // Field appears in the resulting map as key "myName".
//   Field int `cql:"myName"`
//
//   // Field appears in the resulting as key "Field"
//   Field int
//
//   // Field appears in the resulting map as key "myName"
//   Field int "myName"
func StructToMap(val interface{}) (map[string]interface{}, bool) {
	// indirect so function works with both structs and pointers to them
	structVal := r.Indirect(r.ValueOf(val))
	kind := structVal.Kind()
	if kind != r.Struct {
		return nil, false
	}
	sinfo := getStructInfo(structVal)
	mapVal := make(map[string]interface{}, len(sinfo.FieldsList))
	for _, field := range sinfo.FieldsList {
		if structVal.Field(field.Num).CanInterface() {
			mapVal[field.Key] = structVal.Field(field.Num).Interface()
		}
	}
	return mapVal, true
}

// MapToStruct converts a map to a struct. It is the inverse of the StructToMap
// function. For details see StructToMap.
func MapToStruct(m map[string]interface{}, struc interface{}) error {
	//fmt.Printf("Input map: %+v\n", m)
	//fmt.Printf("Input struc: %+v\n", struc)
	val := r.Indirect(r.ValueOf(struc))
	sinfo := getStructInfo(val)
	//fmt.Printf("sinfo: %+v\n", sinfo)
	for k, v := range m {
		//fmt.Printf("k: %+v  v: %+v\n", k, v)
		if info, ok := sinfo.FieldsMap[k]; ok {
			//fmt.Printf("info: %+v\n", info)
			structField := val.Field(info.Num)
			//fmt.Printf("type struct: %q, %q, %q\n", structField.Type(), structField.Type().Name(), structField.Kind())
			//fmt.Printf("type value: %q\n", r.TypeOf(v).Name())
			//fmt.Printf("value: %+v\n", r.ValueOf(v))
			if structField.Kind().String() == "slice" && r.TypeOf(v).Kind().String() == "slice" {
				if structField.Type().Elem() == r.TypeOf(v).Elem() {
					//fmt.Print("Slices of same type\n")
					structField.Set(r.ValueOf(v))
				} else if structField.Type().Elem().Kind().String() == r.TypeOf(v).Elem().Kind().String() {
					//fmt.Print("Slices of same kind\n")
					s := r.ValueOf(v)
					result := r.MakeSlice(structField.Type(), 0, s.Len())
					for j := 0; j < s.Len(); j++ {
						result = r.Append(result, r.ValueOf(s.Index(j).Interface()).Convert(structField.Type().Elem()))
					}
					structField.Set(result)
				} else if r.TypeOf(v).Elem().String() == "string" {
					//fmt.Print("Slices of different kind\n")
					stringList := v.([]string)
					result := r.MakeSlice(structField.Type(), 0, len(stringList))
					for _, str := range stringList {
						tmp := r.New(structField.Type().Elem())
						err := json.Unmarshal([]byte(str), tmp.Interface())
						if err != nil {
							//fmt.Printf("Unmarshal failed on: %q due to: %q!!!\n", str, err)
							//return err
							continue
						}
						result = r.Append(result, r.Indirect(tmp))
					}
					structField.Set(result)
				}
			} else if structField.Type().Name() == "" || r.TypeOf(v).Name() == "" {
				return fmt.Errorf("WTF are these types???!!! %q %q\n", structField.Kind().String(), r.TypeOf(v).Kind().String())
			} else if structField.Type().Name() == r.TypeOf(v).Name() {
				//fmt.Print("Field set naturally!!!\n")
				structField.Set(r.ValueOf(v))
			} else if structField.Kind().String() == r.TypeOf(v).Name() {
				//fmt.Print("Field set with convert !!!\n")
				structField.Set(r.ValueOf(v).Convert(structField.Type()))
			} else {
				return fmt.Errorf("Please handle these types: %s with %s\n", structField.Kind().String(), r.TypeOf(v).Kind().String())
			}
		} else {
			//fmt.Printf("field %q not found\n", k) TODO: in which situation do we reach this point? oO
		}
		//fmt.Printf("Check fill struc: %+v\n", struc)
	}
	return nil
}

// FieldsAndValues returns a list field names and a corresponing list of values
// for the given struct. For details on how the field names are determined please
// see StructToMap.
func FieldsAndValues(val interface{}) ([]string, []interface{}, bool) {
	// indirect so function works with both structs and pointers to them
	structVal := r.Indirect(r.ValueOf(val))
	kind := structVal.Kind()
	if kind != r.Struct {
		return nil, nil, false
	}
	sinfo := getStructInfo(structVal)
	fields := make([]string, len(sinfo.FieldsList))
	values := make([]interface{}, len(sinfo.FieldsList))
	for i, info := range sinfo.FieldsList {
		field := structVal.Field(info.Num)
		fields[i] = info.Key
		values[i] = field.Interface()
	}
	return fields, values, true
}

var structMapMutex sync.RWMutex
var structMap = make(map[r.Type]*structInfo)

type fieldInfo struct {
	Key string
	Num int
}

type structInfo struct {
	// FieldsMap is used to access fields by their key
	FieldsMap map[string]fieldInfo
	// FieldsList allows iteration over the fields in their struct order.
	FieldsList []fieldInfo
}

func getStructInfo(v r.Value) *structInfo {
	st := r.Indirect(v).Type()
	structMapMutex.RLock()
	sinfo, found := structMap[st]
	structMapMutex.RUnlock()
	if found {
		return sinfo
	}

	n := st.NumField()
	fieldsMap := make(map[string]fieldInfo, n)
	fieldsList := make([]fieldInfo, 0, n)
	for i := 0; i != n; i++ {
		field := st.Field(i)
		info := fieldInfo{Num: i}
		tag := field.Tag.Get("cql")
		// If there is no cql specific tag and there are no other tags
		// set the cql tag to the whole field tag
		if tag == "" && strings.Index(string(field.Tag), ":") < 0 {
			tag = string(field.Tag)
		}
		if tag != "" {
			info.Key = tag
		} else {
			info.Key = field.Name
		}

		if _, found = fieldsMap[strings.ToLower(info.Key)]; found {
			msg := fmt.Sprintf("Duplicated key '%s' in struct %s", info.Key, st.String())
			panic(msg)
		}

		fieldsList = append(fieldsList, info)
		fieldsMap[strings.ToLower(info.Key)] = info
	}
	sinfo = &structInfo{
		fieldsMap,
		fieldsList,
	}
	structMapMutex.Lock()
	structMap[st] = sinfo
	structMapMutex.Unlock()
	return sinfo
}
