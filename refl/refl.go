package refl

import "reflect"

func IsAddress(addr interface{}) bool {
	rt := reflect.TypeOf(addr)
	return rt.Kind() == reflect.Ptr
}

func ComposedOf(item interface{}, parent interface{}) bool {

	it := reflect.TypeOf(item)
	if it.Kind() == reflect.Ptr {
		it = it.Elem()
	}

	pt := reflect.TypeOf(parent)
	if pt.Kind() == reflect.Ptr {
		pt = pt.Elem()
	}
	if pt.Kind() != reflect.Struct {
		panic("parent must be struct type")
	}

	// find field with parent's exact name
	f, ok := it.FieldByName(pt.Name())
	if !ok {
		return false
	}

	if !f.Anonymous {
		return false
	}

	if !f.Type.ConvertibleTo(pt) {
		return false
	}

	return true
}

func ObjSignature(o interface{}) string {
	return TypeSignature(reflect.TypeOf(o))
}

func TypeSignature(t reflect.Type) string {
	symb := ""
	if t.Kind() == reflect.Ptr {
		symb = "*" + TypeSignature(t.Elem())
	} else if t.Kind() == reflect.Map {
		symb = "map"
	} else if t.Kind() == reflect.Struct {
		symb = "st:" + t.PkgPath() + "." + t.Name()
	} else if t.Kind() == reflect.Interface {
		symb = "i:" + t.PkgPath() + "." + t.Name()
	} else if t.Kind() == reflect.Array {
		symb = "sl:" + t.Elem().PkgPath() + "." + t.Elem().Name()
	} else if t.Kind() == reflect.Slice {
		symb = "sl:" + t.Elem().PkgPath() + "." + t.Elem().Name()
	} else {
		symb = t.Name()
	}
	return symb
}

func NestedFields(ifc interface{}) []reflect.StructField {
	fields := make([]reflect.StructField, 0)

	ift := reflect.TypeOf(ifc)
	ifv := reflect.ValueOf(ifc)

	for i := 0; i < ift.NumField(); i++ {

		fv := ifv.Field(i)
		ft := ift.Field(i)

		if fv.Kind() == reflect.Struct {
			fields = append(fields, NestedFields(fv.Interface())...)
		} else {
			fields = append(fields, ft)
		}
	}

	return fields
}
