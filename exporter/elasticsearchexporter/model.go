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

package elasticsearchexporter

import (
	"bytes"

	"go.opentelemetry.io/collector/consumer/pdata"

	"./internal/objmodel"
)

type mappingModel interface {
	encodeLog(pdata.LogRecord) ([]byte, error)
}

// encodeModel tries to keep the event as close to the original protobuf as is.
// No fields will be mapped by default.
//
// Field deduplication and dedotting of attributes is supported by the encodeModel.
type encodeModel struct {
	dedup bool
	dedot bool
}

func (m *encodeModel) encodeLog(record pdata.LogRecord) ([]byte, error) {
	// Prepare a JSON document for indexing into Elasticsearch. We try to stay close to the
	// protobuf message here.
	// See: https://github.com/open-telemetry/oteps/blob/master/text/logs/0097-log-data-model.md
	//
	// Although the AttributeMap wrapper tries to make the attributes table appear like a table,
	// it is in fact an array of key value pairs on the wire protocol. The array can include duplicate keys,
	// which is not unusual for 'high'-performance loggers that just add/encode
	// new keys to a list of kv-pairs (e.g. zap). The Elasticsearch JSON parser
	// can not handle duplicate keys, so we try to filter them out when `dedup`
	// is enabled. In case of duplicates the AttributeMap API favors the first
	// key in the table, while append-only loggers would favor the last entry. The encoder
	// will priotize later keys as well.
	// The `.` is special to Elasticsearch. In order to handle common prefixes and attributes
	// being a mix of key value pairs with dots and complex objects, we flatten the document first
	// before we deduplicate. Final dedotting is optional an only required when
	// Ingest Node is used. But either way, we try to present a well formed
	// document to Elasticsearch.

	var document objmodel.Document

	document.AddTimestamp("Timestamp", record.Timestamp())
	document.AddID("TraceId", record.TraceID())
	document.AddID("SpanId", record.SpanID())
	document.AddInt("TraceFlags", int64(record.Flags()))
	document.AddString("SeverityText", record.SeverityText())
	document.AddInt("SeverityNumber", int64(record.SeverityNumber()))
	document.AddString("Name", record.Name())

	switch {
	case record.Body().Type() == pdata.AttributeValueMAP:
		document = objFromAttributesWithPath("Body.", record.Body().MapVal())
	case record.Body().Type() == pdata.AttributeValueNULL:
		document = object{}
	default:
		document = object{}
		document.Add("Body", valueFromAttribute(record.Body()))
	}

	document.AddAttributes("Attributes", record.Attributes())

	// TODO: The specification mentions a 'Resource' namespace.
	//       Figure out how to access those resources from the pdata.Logs

	document.sort()

	if m.dedup {
		document.dedup()
	}

	var buf bytes.Buffer
	err := document.Serialize(&buf, m.dedot)
	return buf.Bytes(), err
}
