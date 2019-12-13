// Copyright 2019, OpenTelemetry Authors
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

package jaegerreceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	apacheThrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/gorilla/mux"
	"github.com/jaegertracing/jaeger/cmd/agent/app/configmanager"
	"github.com/jaegertracing/jaeger/cmd/agent/app/httpserver"
	"github.com/jaegertracing/jaeger/cmd/agent/app/processors"
	"github.com/jaegertracing/jaeger/cmd/agent/app/reporter"
	"github.com/jaegertracing/jaeger/cmd/agent/app/servers"
	"github.com/jaegertracing/jaeger/cmd/agent/app/servers/thriftudp"
	"github.com/jaegertracing/jaeger/cmd/collector/app"
	"github.com/jaegertracing/jaeger/proto-gen/api_v2"
	"github.com/jaegertracing/jaeger/thrift-gen/baggage"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	jaegerThrift "github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger/thrift-gen/sampling"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector/consumer"
	"github.com/open-telemetry/opentelemetry-collector/observability"
	"github.com/open-telemetry/opentelemetry-collector/oterr"
	"github.com/open-telemetry/opentelemetry-collector/receiver"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector/translator/trace/jaeger"
)

// Configuration defines the behavior and the ports that
// the Jaeger receiver will use.
type Configuration struct {
	CollectorThriftPort  int
	CollectorHTTPPort    int
	CollectorGRPCPort    int
	CollectorGRPCOptions []grpc.ServerOption

	AgentCompactThriftPort int
	AgentBinaryThriftPort  int
	AgentHTTPPort          int
}

// Receiver type is used to receive spans that were originally intended to be sent to Jaeger.
// This receiver is basically a Jaeger collector.
type jReceiver struct {
	// mu protects the fields of this type
	mu sync.Mutex

	nextConsumer consumer.TraceConsumer

	startOnce sync.Once
	stopOnce  sync.Once

	config *Configuration

	grpc            *grpc.Server
	tchanServer     *jTchannelReceiver
	collectorServer *http.Server

	agentProcessors []processors.Processor
	agentServer     *http.Server

	defaultAgentCtx context.Context
	logger          *zap.Logger
}

type jTchannelReceiver struct {
	nextConsumer consumer.TraceConsumer

	tchannel *tchannel.Channel
}

const (
	defaultAgentQueueSize     = 1000
	defaultAgentMaxPacketSize = 65000
	defaultAgentServerWorkers = 10

	// As per https://www.jaegertracing.io/docs/1.13/deployment/
	// By default, the port used by jaeger-agent to send spans in model.proto format
	defaultGRPCPort = 14250
	// By default, the port used by jaeger-agent to send spans in jaeger.thrift format
	defaultTChannelPort = 14267
	// By default, can accept spans directly from clients in jaeger.thrift format over binary thrift protocol
	defaultCollectorHTTPPort = 14268

	traceSource string = "Jaeger"
)

// New creates a TraceReceiver that receives traffic as a collector with both Thrift and HTTP transports.
func New(ctx context.Context, config *Configuration, nextConsumer consumer.TraceConsumer, logger *zap.Logger) (receiver.TraceReceiver, error) {
	return &jReceiver{
		config:          config,
		defaultAgentCtx: observability.ContextWithReceiverName(context.Background(), "jaeger-agent"),
		nextConsumer:    nextConsumer,
		tchanServer: &jTchannelReceiver{
			nextConsumer: nextConsumer,
		},
		logger: logger,
	}, nil
}

var _ receiver.TraceReceiver = (*jReceiver)(nil)

func (jr *jReceiver) collectorAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.CollectorHTTPPort
	}
	if port <= 0 {
		port = defaultCollectorHTTPPort
	}
	return fmt.Sprintf(":%d", port)
}

// TODO https://github.com/open-telemetry/opentelemetry-collector/issues/267
//	Remove ThriftTChannel support.
func (jr *jReceiver) tchannelAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.CollectorThriftPort
	}
	if port <= 0 {
		port = defaultTChannelPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *jReceiver) grpcAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.CollectorGRPCPort
	}
	if port <= 0 {
		port = defaultGRPCPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *jReceiver) agentCompactThriftAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.AgentCompactThriftPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *jReceiver) agentCompactThriftEnabled() bool {
	return jr.config != nil && jr.config.AgentCompactThriftPort > 0
}

func (jr *jReceiver) agentBinaryThriftAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.AgentBinaryThriftPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *jReceiver) agentBinaryThriftEnabled() bool {
	return jr.config != nil && jr.config.AgentBinaryThriftPort > 0
}

func (jr *jReceiver) agentHTTPPortAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.AgentHTTPPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *jReceiver) agentHTTPEnabled() bool {
	return jr.config != nil && jr.config.AgentHTTPPort > 0
}

func (jr *jReceiver) TraceSource() string {
	return traceSource
}

func (jr *jReceiver) StartTraceReception(host receiver.Host) error {
	jr.mu.Lock()
	defer jr.mu.Unlock()

	var err = oterr.ErrAlreadyStarted
	jr.startOnce.Do(func() {
		if err = jr.startAgent(host); err != nil && err != oterr.ErrAlreadyStarted {
			jr.stopTraceReceptionLocked()
			return
		}

		if err = jr.startCollector(host); err != nil && err != oterr.ErrAlreadyStarted {
			jr.stopTraceReceptionLocked()
			return
		}

		err = nil
	})
	return err
}

func (jr *jReceiver) StopTraceReception() error {
	jr.mu.Lock()
	defer jr.mu.Unlock()

	return jr.stopTraceReceptionLocked()
}

func (jr *jReceiver) stopTraceReceptionLocked() error {
	var err = oterr.ErrAlreadyStopped
	jr.stopOnce.Do(func() {
		var errs []error

		if jr.agentServer != nil {
			if aerr := jr.agentServer.Close(); aerr != nil {
				errs = append(errs, aerr)
			}
			jr.agentServer = nil
		}
		for _, processor := range jr.agentProcessors {
			processor.Stop()
		}

		if jr.collectorServer != nil {
			if cerr := jr.collectorServer.Close(); cerr != nil {
				errs = append(errs, cerr)
			}
			jr.collectorServer = nil
		}
		if jr.tchanServer.tchannel != nil {
			jr.tchanServer.tchannel.Close()
			jr.tchanServer.tchannel = nil
		}
		if jr.grpc != nil {
			jr.grpc.Stop()
			jr.grpc = nil
		}
		if len(errs) == 0 {
			err = nil
			return
		}
		// Otherwise combine all these errors
		buf := new(bytes.Buffer)
		for _, err := range errs {
			fmt.Fprintf(buf, "%s\n", err.Error())
		}
		err = errors.New(buf.String())
	})

	return err
}

const collectorReceiverTagValue = "jaeger-collector"
const tchannelCollectorReceiverTagValue = "jaeger-tchannel-collector"

func consumeTraceData(ctx context.Context, batches []*jaeger.Batch, consumer consumer.TraceConsumer) ([]*jaeger.BatchSubmitResponse, error) {
	jbsr := make([]*jaeger.BatchSubmitResponse, 0, len(batches))

	for _, batch := range batches {
		td, err := jaegertranslator.ThriftBatchToOCProto(batch)
		// TODO: (@odeke-em) add this error for Jaeger observability
		ok := false

		if err == nil {
			ok = true
			td.SourceFormat = "jaeger"
			consumer.ConsumeTraceData(ctx, td)
			// We MUST unconditionally record metrics from this reception.
			observability.RecordMetricsForTraceReceiver(ctx, len(batch.Spans), len(batch.Spans)-len(td.Spans))
		}

		jbsr = append(jbsr, &jaeger.BatchSubmitResponse{
			Ok: ok,
		})
	}

	return jbsr, nil
}

func (jr *jReceiver) SubmitBatches(batches []*jaeger.Batch, options app.SubmitBatchOptions) ([]*jaeger.BatchSubmitResponse, error) {
	ctx := context.Background()
	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, collectorReceiverTagValue)

	return consumeTraceData(ctxWithReceiverName, batches, jr.nextConsumer)
}

func (jtr *jTchannelReceiver) SubmitBatches(ctx thrift.Context, batches []*jaeger.Batch) ([]*jaeger.BatchSubmitResponse, error) {
	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, tchannelCollectorReceiverTagValue)

	return consumeTraceData(ctxWithReceiverName, batches, jtr.nextConsumer)
}

var _ reporter.Reporter = (*jReceiver)(nil)
var _ api_v2.CollectorServiceServer = (*jReceiver)(nil)
var _ configmanager.ClientConfigManager = (*jReceiver)(nil)

// EmitZipkinBatch implements cmd/agent/reporter.Reporter and it forwards
// Zipkin spans received by the Jaeger agent processor.
func (jr *jReceiver) EmitZipkinBatch(spans []*zipkincore.Span) error {
	return nil
}

// EmitBatch implements cmd/agent/reporter.Reporter and it forwards
// Jaeger spans received by the Jaeger agent processor.
func (jr *jReceiver) EmitBatch(batch *jaeger.Batch) error {
	td, err := jaegertranslator.ThriftBatchToOCProto(batch)
	td.SourceFormat = "jaeger"
	if err != nil {
		observability.RecordMetricsForTraceReceiver(jr.defaultAgentCtx, len(batch.Spans), len(batch.Spans))
		return err
	}

	err = jr.nextConsumer.ConsumeTraceData(jr.defaultAgentCtx, td)
	observability.RecordMetricsForTraceReceiver(jr.defaultAgentCtx, len(batch.Spans), len(batch.Spans)-len(td.Spans))

	return err
}

func (jr *jReceiver) GetSamplingStrategy(serviceName string) (*sampling.SamplingStrategyResponse, error) {
	return &sampling.SamplingStrategyResponse{}, nil
}

func (jr *jReceiver) GetBaggageRestrictions(serviceName string) ([]*baggage.BaggageRestriction, error) {
	return nil, nil
}

func (jr *jReceiver) PostSpans(ctx context.Context, r *api_v2.PostSpansRequest) (*api_v2.PostSpansResponse, error) {
	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, collectorReceiverTagValue)

	td, err := jaegertranslator.ProtoBatchToOCProto(r.Batch)
	td.SourceFormat = "jaeger"
	if err != nil {
		observability.RecordMetricsForTraceReceiver(ctxWithReceiverName, len(r.Batch.Spans), len(r.Batch.Spans))
		return nil, err
	}

	err = jr.nextConsumer.ConsumeTraceData(ctx, td)
	observability.RecordMetricsForTraceReceiver(ctxWithReceiverName, len(r.Batch.Spans), len(r.Batch.Spans)-len(td.Spans))
	if err != nil {
		return nil, err
	}

	return &api_v2.PostSpansResponse{}, err
}

func (jr *jReceiver) startAgent(_ receiver.Host) error {
	if !jr.agentBinaryThriftEnabled() && !jr.agentCompactThriftEnabled() && !jr.agentHTTPEnabled() {
		return nil
	}

	if jr.agentBinaryThriftEnabled() {
		processor, err := jr.buildProcessor(jr.agentBinaryThriftAddr(), apacheThrift.NewTBinaryProtocolFactoryDefault())
		if err != nil {
			return err
		}
		jr.agentProcessors = append(jr.agentProcessors, processor)
	}

	if jr.agentCompactThriftEnabled() {
		processor, err := jr.buildProcessor(jr.agentCompactThriftAddr(), apacheThrift.NewTCompactProtocolFactory())
		if err != nil {
			return err
		}
		jr.agentProcessors = append(jr.agentProcessors, processor)
	}

	for _, processor := range jr.agentProcessors {
		go processor.Serve()
	}

	if jr.agentHTTPEnabled() {
		jr.agentServer = httpserver.NewHTTPServer(jr.agentHTTPPortAddr(), jr, metrics.NullFactory)

		go func() {
			if err := jr.agentServer.ListenAndServe(); err != nil {
				jr.logger.Error("http server failure", zap.Error(err))
			}
		}()
	}

	return nil
}

func (jr *jReceiver) buildProcessor(address string, factory apacheThrift.TProtocolFactory) (processors.Processor, error) {
	handler := jaegerThrift.NewAgentProcessor(jr)
	transport, err := thriftudp.NewTUDPServerTransport(address)
	if err != nil {
		return nil, err
	}
	server, err := servers.NewTBufferedServer(transport, defaultAgentQueueSize, defaultAgentMaxPacketSize, metrics.NullFactory)
	if err != nil {
		return nil, err
	}
	processor, err := processors.NewThriftProcessor(server, defaultAgentServerWorkers, metrics.NullFactory, factory, handler, jr.logger)
	if err != nil {
		return nil, err
	}
	return processor, nil
}

func (jr *jReceiver) startCollector(host receiver.Host) error {
	tch, terr := tchannel.NewChannel("jaeger-collector", new(tchannel.ChannelOptions))
	if terr != nil {
		return fmt.Errorf("failed to create NewTChannel: %v", terr)
	}

	server := thrift.NewServer(tch)
	server.Register(jaeger.NewTChanCollectorServer(jr.tchanServer))

	taddr := jr.tchannelAddr()
	tln, terr := net.Listen("tcp", taddr)
	if terr != nil {
		return fmt.Errorf("failed to bind to TChannel address %q: %v", taddr, terr)
	}
	tch.Serve(tln)
	jr.tchanServer.tchannel = tch

	// Now the collector that runs over HTTP
	caddr := jr.collectorAddr()
	cln, cerr := net.Listen("tcp", caddr)
	if cerr != nil {
		// Abort and close tch
		tch.Close()
		return fmt.Errorf("failed to bind to Collector address %q: %v", caddr, cerr)
	}

	nr := mux.NewRouter()
	apiHandler := app.NewAPIHandler(jr)
	apiHandler.RegisterRoutes(nr)
	jr.collectorServer = &http.Server{Handler: nr}
	go func() {
		_ = jr.collectorServer.Serve(cln)
	}()

	jr.grpc = grpc.NewServer(jr.config.CollectorGRPCOptions...)
	gaddr := jr.grpcAddr()
	gln, gerr := net.Listen("tcp", gaddr)
	if gerr != nil {
		// Abort and close tch, cln
		tch.Close()
		cln.Close()
		return fmt.Errorf("failed to bind to gRPC address %q: %v", gaddr, gerr)
	}

	api_v2.RegisterCollectorServiceServer(jr.grpc, jr)

	go func() {
		if err := jr.grpc.Serve(gln); err != nil {
			host.ReportFatalError(err)
		}
	}()

	return nil
}