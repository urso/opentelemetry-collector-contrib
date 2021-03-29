// Copyright 2021, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objmodel

import (
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/elastic/go-structform"
	"github.com/elastic/go-structform/json"
	"go.opentelemetry.io/collector/consumer/pdata"
)

type Document struct {
	fields []field
}

type field struct {
	key   string
	value Value
}

type Value struct {
	kind      Kind
	primitive uint64
	dbl       float64
	str       string
	arr       []Value
	doc       Document
	ts        time.Time
}

type Kind uint8

const (
	KindNil Kind = iota
	KindBool
	KindInt
	KindDouble
	KindString
	KindArr
	KindObject
	KindTimestamp
	KindIgnore
)

const tsLayout = "2006-01-02T15:04:05.000000000Z"

var nilValue = Value{kind: KindNil}
var ignoreValue = Value{kind: KindIgnore}

func DocumentFromAttributes(am pdata.AttributeMap) Document {
	return DocumentFromAttributesWithPath("", am)
}

func DocumentFromAttributesWithPath(path string, am pdata.AttributeMap) Document {
	if am.Len() == 0 {
		return Document{}
	}

	fields := make([]field, 0, am.Len())
	fields = appendAttributeFields(fields, path, am)
	return Document{fields}
}

func (doc *Document) AddTimestamp(key string, ts pdata.Timestamp) {
	doc.Add(key, TimestampValue(ts.AsTime()))
}

func (doc *Document) Add(key string, v Value) {
	doc.fields = append(doc.fields, field{key: key, value: v})
}

func (doc *Document) AddString(key string, v string) {
	if v != "" {
		doc.Add(key, StringValue(v))
	}
}

func (doc *Document) AddID(key string, id interface {
	IsEmpty() bool
	HexString() string
}) {
	if !id.IsEmpty() {
		doc.AddString(key, id.HexString())
	}
}

func (doc *Document) AddInt(key string, value int64) {
	doc.Add(key, IntValue(value))
}

func (doc *Document) AddAttributes(key string, attributes pdata.AttributeMap) {
	doc.fields = appendAttributeFields(doc.fields, key, attributes)
}

func (doc *Document) Sort() {
	sort.SliceStable(doc.fields, func(i, j int) bool {
		return doc.fields[i].key < doc.fields[j].key
	})

	for i := range doc.fields {
		fld := &doc.fields[i]
		fld.value.Sort()
	}
}

func (doc *Document) Dedup() {
	// 1. rename fields if a primitive value is overwritten by an object.
	//    For example the pair (path.x=1, path.x.a="test") becomes:
	//    (path.x.value=1, path.x.a="test").
	//
	//    This step removes potential conflicts when dedotting and serializing fields.
	for i := 0; i < len(doc.fields)-1; i++ {
		if len(doc.fields[i].key) < len(doc.fields[i+1].key) &&
			strings.HasPrefix(doc.fields[i+1].key, doc.fields[i].key) {
			doc.fields[i].key = doc.fields[i].key + ".value"
		}
	}

	// 2. mark duplicates as 'ignore'
	//
	//    This step ensures that we do not have duplicate fields names when serializing.
	//    Elasticsearch JSON parser will fail otherwise.
	for i := 0; i < len(doc.fields)-1; i++ {
		if doc.fields[i].key == doc.fields[i+1].key {
			doc.fields[i].value = ignoreValue
		}
	}

	// 3. fix objects that might be stored in arrays
	for i := range doc.fields {
		doc.fields[i].value.Dedup()
	}
}

func (doc *Document) Serialize(w io.Writer, dedot bool) error {
	v := json.NewVisitor(w)
	return doc.iterJSON(v, dedot)
}

func (doc *Document) iterJSON(v *json.Visitor, dedot bool) error {
	if dedot {
		return doc.iterJSONDedot(v)
	}
	return doc.iterJSONFlat(v)
}

func (doc *Document) iterJSONFlat(w *json.Visitor) error {
	w.OnObjectStart(-1, structform.AnyType)
	defer w.OnObjectFinished()

	for i := range doc.fields {
		fld := &doc.fields[i]

		// filter out empty values
		if fld.value.kind == KindIgnore ||
			fld.value.kind == KindNil ||
			(fld.value.kind == KindArr && len(fld.value.arr) == 0) {
			continue
		}

		w.OnKey(fld.key)
		if err := fld.value.iterJSON(w, true); err != nil {
			return err
		}
	}

	return nil
}

func (doc *Document) iterJSONDedot(w *json.Visitor) error {
	return errors.New("TODO")
}

func StringValue(str string) Value { return Value{kind: KindString, str: str} }

func IntValue(i int64) Value { return Value{kind: KindInt, primitive: uint64(i)} }

func DoubleValue(d float64) Value { return Value{kind: KindDouble, dbl: d} }

func BoolValue(b bool) Value {
	var v uint64
	if b {
		v = 1
	}
	return Value{kind: KindBool, primitive: v}
}

func ArrValue(values ...Value) Value {
	return Value{kind: KindArr, arr: values}
}

func TimestampValue(ts time.Time) Value {
	return Value{kind: KindTimestamp, ts: ts}
}

func ValueFromAttribute(attr pdata.AttributeValue) Value {
	switch attr.Type() {
	case pdata.AttributeValueINT:
		return IntValue(attr.IntVal())
	case pdata.AttributeValueDOUBLE:
		return DoubleValue(attr.DoubleVal())
	case pdata.AttributeValueSTRING:
		return StringValue(attr.StringVal())
	case pdata.AttributeValueBOOL:
		return BoolValue(attr.BoolVal())
	case pdata.AttributeValueARRAY:
		sub := arrFromAttributes(attr.ArrayVal())
		return ArrValue(sub...)
	case pdata.AttributeValueMAP:
		sub := DocumentFromAttributes(attr.MapVal())
		return Value{kind: KindObject, doc: sub}
	default:
		return nilValue
	}
}

func (v *Value) Sort() {
	switch v.kind {
	case KindObject:
		v.doc.Sort()
	case KindArr:
		for i := range v.arr {
			v.arr[i].Sort()
		}
	}
}

func (v *Value) Dedup() {
	switch v.kind {
	case KindObject:
		v.doc.Dedup()
	case KindArr:
		for i := range v.arr {
			v.arr[i].Dedup()
		}
	}
}

func (v *Value) iterJSON(w *json.Visitor, dedot bool) error {
	switch v.kind {
	case KindNil:
		return w.OnNil()
	case KindBool:
		return w.OnBool(v.primitive == 1)
	case KindInt:
		return w.OnInt64(int64(v.primitive))
	case KindDouble:
		if math.IsNaN(v.dbl) || math.IsInf(v.dbl, 0) {
			// NaN and Inf are undefined for JSON. Let's serialize to "null"
			return w.OnNil()
		}
		return w.OnFloat64(v.dbl)
	case KindString:
		return w.OnString(v.str)
	case KindTimestamp:
		str := v.ts.UTC().Format(tsLayout)
		return w.OnString(str)
	case KindObject:
		if len(v.doc.fields) == 0 {
			return w.OnNil()
		}
		return v.doc.iterJSON(w, dedot)
	case KindArr:
		w.OnArrayStart(-1, structform.AnyType)
		for i := range v.arr {
			if err := v.arr[i].iterJSON(w, dedot); err != nil {
				return err
			}
		}
		w.OnArrayFinished()
	}

	return nil
}

func arrFromAttributes(aa pdata.AnyValueArray) []Value {
	if aa.Len() == 0 {
		return nil
	}

	values := make([]Value, aa.Len())
	for i := 0; i < aa.Len(); i++ {
		values[i] = ValueFromAttribute(aa.At(i))
	}
	return values
}

func appendAttributeFields(fields []field, path string, am pdata.AttributeMap) []field {
	am.ForEach(func(k string, val pdata.AttributeValue) {
		fields = appendAttributeValue(fields, path, k, val)
	})
	return fields
}

func appendAttributeValue(fields []field, path string, key string, attr pdata.AttributeValue) []field {
	if attr.Type() == pdata.AttributeValueNULL {
		return fields
	}

	if attr.Type() == pdata.AttributeValueMAP {
		return appendAttributeFields(fields, flattenKey(path, key), attr.MapVal())
	}

	return append(fields, field{
		key:   flattenKey(path, key),
		value: ValueFromAttribute(attr),
	})
}

func flattenKey(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
