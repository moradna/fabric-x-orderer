/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	popOrWaitLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "pop_or_wait_latency_seconds",
		Help:       "The latency of PopOrWait to retrieve the requested batch.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	baToBatchLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "ba_to_batch_latency_seconds",
		Help:       "The latency from receiving a batch attestation until the matching batch is available.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	batchToLedgerLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "batch_to_ledger_latency_seconds",
		Help:       "The latency from having the matching batch available until it is appended to the ledger.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}
)

type Metrics struct {
	ledgerMetrics        *node_ledger.AssemblerLedgerMetrics
	deliverMetrics       *deliver.Metrics
	popOrWaitLatency     metrics.Histogram
	baToBatchLatency     metrics.Histogram
	batchToLedgerLatency metrics.Histogram
	logger               *flogging.FabricLogger
	interval             time.Duration
	stopChan             chan struct{}
	stopOnce             sync.Once
	startOnce            sync.Once
	partyID              arma_types.PartyID
}

func NewMetrics(assemblerNodeConfig *config.AssemblerNodeConfig, ledgerMetrics *node_ledger.AssemblerLedgerMetrics, logger *flogging.FabricLogger) *Metrics {
	partyID := fmt.Sprintf("%d", assemblerNodeConfig.PartyId)

	provider := monitoring.NewProvider(assemblerNodeConfig.Metrics.Provider, logger)

	versionGauge := monitoring.VersionGauge(provider)
	versionGauge.With(metadata.Version).Set(1)

	ledgerMetrics.NewAssemblerLedgerMetrics(provider, partyID, logger)
	deliverMetrics := deliver.NewMetrics(provider)

	popOrWaitLatency := provider.NewHistogram(popOrWaitLatencyOpts).With([]string{partyID}...)
	baToBatchLatency := provider.NewHistogram(baToBatchLatencyOpts).With([]string{partyID}...)
	batchToLedgerLatency := provider.NewHistogram(batchToLedgerLatencyOpts).With([]string{partyID}...)

	return &Metrics{
		ledgerMetrics:        ledgerMetrics,
		deliverMetrics:       deliverMetrics,
		interval:             assemblerNodeConfig.Metrics.MetricsLogInterval,
		logger:               logger,
		stopChan:             make(chan struct{}),
		partyID:              assemblerNodeConfig.PartyId,
		popOrWaitLatency:     popOrWaitLatency,
		baToBatchLatency:     baToBatchLatency,
		batchToLedgerLatency: batchToLedgerLatency,
	}
}

func (m *Metrics) StartMetricsTracker() {
	m.startOnce.Do(func() {
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *Metrics) StopMetricsTracker() {
	m.stopOnce.Do(func() {
		m.logger.Infof("Reporting routine is stopping")
		close(m.stopChan)

		txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
		blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
		blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))

		popOrWaitLatencyAvg := monitoring.GetHistogramAverage(m.popOrWaitLatency.(prometheus.Metric), m.logger)
		baToBatchLatencyAvg := monitoring.GetHistogramAverage(m.baToBatchLatency.(prometheus.Metric), m.logger)
		batchToLedgerLatencyAvg := monitoring.GetHistogramAverage(m.batchToLedgerLatency.(prometheus.Metric), m.logger)

		m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, total: TXs=%d, blocks=%d, estimated_block_size=%d, pop_or_wait_latency_avg=%.6f, ba_to_batch_latency_avg=%.6f, batch_to_ledger_latency_avg=%.6f", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, popOrWaitLatencyAvg, baToBatchLatencyAvg, batchToLedgerLatencyAvg)
	})
}

func (m *Metrics) trackMetrics() {
	lastTxCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
	lastBlocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
			blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
			blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))

			newBlocks := uint64(0)
			if blocksCommitted > lastBlocksCommitted {
				newBlocks = blocksCommitted - lastBlocksCommitted
			}

			newTXs := uint64(0)
			if txCommitted > lastTxCommitted {
				newTXs = txCommitted - lastTxCommitted
			}

			popOrWaitLatencyAvg := monitoring.GetHistogramAverage(m.popOrWaitLatency.(prometheus.Metric), m.logger)
			baToBatchLatencyAvg := monitoring.GetHistogramAverage(m.baToBatchLatency.(prometheus.Metric), m.logger)
			batchToLedgerLatencyAvg := monitoring.GetHistogramAverage(m.batchToLedgerLatency.(prometheus.Metric), m.logger)

			m.logger.Infof("ASSEMBLER_METRICS: total: party_id=%d, TXs=%d, blocks=%d, estimated_block_size=%d, pop_or_wait_latency_avg=%.6f, ba_to_batch_latency_avg=%.6f, batch_to_ledger_latency_avg=%.6f, in the last %.2f seconds: TXs=%d, blocks=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, popOrWaitLatencyAvg, baToBatchLatencyAvg, batchToLedgerLatencyAvg, sec, newTXs, newBlocks)
			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
		case <-m.stopChan:
			return
		}
	}
}
