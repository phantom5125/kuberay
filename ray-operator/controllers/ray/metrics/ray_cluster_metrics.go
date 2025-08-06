package metrics

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

//go:generate mockgen -destination=mocks/ray_cluster_metrics_mock.go -package=mocks github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics RayClusterMetricsObserver
type RayClusterMetricsObserver interface {
	ObserveRayClusterProvisionedDuration(name, namespace string, duration float64)
}

// RayClusterMetricCleanupItem represents an item in the RayCluster metric cleanup queue
type RayClusterMetricCleanupItem struct {
	Name      string
	Namespace string
	DeleteAt  time.Time
}

// RayClusterMetricsManager implements the prometheus.Collector and RayClusterMetricsObserver interface to collect ray cluster metrics.
type RayClusterMetricsManager struct {
	rayClusterProvisionedDurationSeconds *prometheus.GaugeVec
	rayClusterInfo                       *prometheus.Desc
	rayClusterConditionProvisioned       *prometheus.Desc
	client                               client.Client
	log                                  logr.Logger

	// Cleanup queue and related fields specific to RayCluster metrics
	cleanupQueue []RayClusterMetricCleanupItem
	queueMutex   sync.Mutex
	metricTTL    time.Duration
}

// NewRayClusterMetricsManager creates a new RayClusterMetricsManager instance.
func NewRayClusterMetricsManager(ctx context.Context, client client.Client) *RayClusterMetricsManager {
	manager := &RayClusterMetricsManager{
		rayClusterProvisionedDurationSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kuberay_cluster_provisioned_duration_seconds",
				Help: "The time, in seconds, when a RayCluster's `RayClusterProvisioned` status transitions from false (or unset) to true",
			},
			[]string{"name", "namespace"},
		),
		// rayClusterInfo is a gauge metric that indicates the metadata information about RayCluster custom resources.
		// The `owner_kind` label indicates the CRD type that originated the RayCluster.
		// Possible values for `owner_kind`:
		// - None: Created by a RayCluster CRD
		// - RayJob: Created by a RayJob CRD
		// - RayService: Created by a RayService CRD
		rayClusterInfo: prometheus.NewDesc(
			"kuberay_cluster_info",
			"Metadata information about RayCluster custom resources",
			[]string{"name", "namespace", "owner_kind"},
			nil,
		),
		rayClusterConditionProvisioned: prometheus.NewDesc(
			"kuberay_cluster_condition_provisioned",
			"Indicates whether the RayCluster is provisioned",
			[]string{"name", "namespace", "condition"},
			nil,
		),
		client:       client,
		log:          ctrl.LoggerFrom(ctx),
		cleanupQueue: make([]RayClusterMetricCleanupItem, 0),
		metricTTL:    5 * time.Minute, // Keep metrics for 5 minutes
	}

	// Start the cleanup goroutine
	go manager.startRayClusterCleanupLoop(ctx)
	return manager
}

// Describe implements prometheus.Collector interface Describe method.
func (r *RayClusterMetricsManager) Describe(ch chan<- *prometheus.Desc) {
	r.rayClusterProvisionedDurationSeconds.Describe(ch)
	ch <- r.rayClusterInfo
}

// Collect implements prometheus.Collector interface Collect method.
func (r *RayClusterMetricsManager) Collect(ch chan<- prometheus.Metric) {
	r.rayClusterProvisionedDurationSeconds.Collect(ch)

	var rayClusterList rayv1.RayClusterList
	err := r.client.List(context.Background(), &rayClusterList)
	if err != nil {
		r.log.Error(err, "Failed to list RayClusters")
		return
	}

	for _, rayCluster := range rayClusterList.Items {
		r.collectRayClusterInfo(&rayCluster, ch)
		r.collectRayClusterConditionProvisioned(&rayCluster, ch)
	}
}

// ObserveRayClusterProvisionedDuration records the provisioned duration of a RayCluster
func (r *RayClusterMetricsManager) ObserveRayClusterProvisionedDuration(name, namespace string, duration float64) {
	r.rayClusterProvisionedDurationSeconds.WithLabelValues(name, namespace).Set(duration)
}

// ScheduleRayClusterMetricForCleanup schedules a RayCluster metric for cleanup after the TTL
func (r *RayClusterMetricsManager) ScheduleRayClusterMetricForCleanup(name, namespace string) {
	r.queueMutex.Lock()
	defer r.queueMutex.Unlock()

	// Add to cleanup queue
	item := RayClusterMetricCleanupItem{
		Name:      name,
		Namespace: namespace,
		DeleteAt:  time.Now().Add(r.metricTTL),
	}
	r.cleanupQueue = append(r.cleanupQueue, item)
	r.log.Info("Scheduled RayCluster metric for cleanup", "name", name, "namespace", namespace, "deleteAt", item.DeleteAt)
}

// startRayClusterCleanupLoop starts a loop to clean up expired RayCluster metrics
func (r *RayClusterMetricsManager) startRayClusterCleanupLoop(ctx context.Context) {
	// Check for expired metrics every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanupExpiredRayClusterMetrics()
		case <-ctx.Done():
			return
		}
	}
}

// cleanupExpiredRayClusterMetrics removes RayCluster metrics that have expired
func (r *RayClusterMetricsManager) cleanupExpiredRayClusterMetrics() {
	r.queueMutex.Lock()
	defer r.queueMutex.Unlock()

	now := time.Now()
	remainingItems := make([]RayClusterMetricCleanupItem, 0)

	for _, item := range r.cleanupQueue {
		if now.After(item.DeleteAt) {
			// Remove expired metric
			r.rayClusterProvisionedDurationSeconds.DeletePartialMatch(prometheus.Labels{"name": item.Name, "namespace": item.Namespace})
			r.log.Info("Cleaned up expired RayCluster metric", "name", item.Name, "namespace", item.Namespace)
		} else {
			// Keep non-expired items
			remainingItems = append(remainingItems, item)
		}
	}

	// Update queue
	r.cleanupQueue = remainingItems
}

func (r *RayClusterMetricsManager) collectRayClusterInfo(cluster *rayv1.RayCluster, ch chan<- prometheus.Metric) {
	ownerKind := "None"
	if v, ok := cluster.Labels[utils.RayOriginatedFromCRDLabelKey]; ok {
		ownerKind = v
	}

	ch <- prometheus.MustNewConstMetric(
		r.rayClusterInfo,
		prometheus.GaugeValue,
		1,
		cluster.Name,
		cluster.Namespace,
		ownerKind,
	)
}

func (r *RayClusterMetricsManager) collectRayClusterConditionProvisioned(cluster *rayv1.RayCluster, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		r.rayClusterConditionProvisioned,
		prometheus.GaugeValue,
		1,
		cluster.Name,
		cluster.Namespace,
		strconv.FormatBool(meta.IsStatusConditionTrue(cluster.Status.Conditions, string(rayv1.RayClusterProvisioned))),
	)
}
