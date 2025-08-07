package metrics

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

//go:generate mockgen -destination=mocks/ray_job_metrics_mock.go -package=mocks github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics RayJobMetricsObserver
type RayJobMetricsObserver interface {
	ObserveRayJobExecutionDuration(name, namespace string, jobDeploymentStatus rayv1.JobDeploymentStatus, retryCount int, duration float64)
}

// RayJobMetricCleanupItem represents an item in the RayJob metric cleanup queue
type RayJobMetricCleanupItem struct {
	Name      string
	Namespace string
	DeleteAt  time.Time
}

// RayJobMetricsManager implements the prometheus.Collector and RayJobMetricsObserver interface to collect ray job metrics.
type RayJobMetricsManager struct {
	rayJobExecutionDurationSeconds *prometheus.GaugeVec
	rayJobInfo                     *prometheus.Desc
	rayJobDeploymentStatus         *prometheus.Desc
	client                         client.Client
	log                            logr.Logger

	// Cleanup queue and related fields specific to RayJob metrics
	cleanupQueue []RayJobMetricCleanupItem
	queueMutex   sync.Mutex
	metricTTL    time.Duration
}

// NewRayJobMetricsManager creates a new RayJobMetricsManager instance.
func NewRayJobMetricsManager(ctx context.Context, client client.Client, metricsTTLSeconds int) *RayJobMetricsManager {
	manager := &RayJobMetricsManager{
		rayJobExecutionDurationSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kuberay_job_execution_duration_seconds",
				Help: "Duration from when the RayJob CR’s JobDeploymentStatus transitions from Initializing to either the Retrying state or a terminal state, such as Complete or Failed. The Retrying state indicates that the CR previously failed and that spec.backoffLimit is enabled.",
			},
			[]string{"name", "namespace", "job_deployment_status", "retry_count"},
		),
		// rayJobInfo is a gauge metric that indicates the metadata information about RayJob custom resources.
		rayJobInfo: prometheus.NewDesc(
			"kuberay_job_info",
			"Metadata information about RayJob custom resources",
			[]string{"name", "namespace"},
			nil,
		),
		// rayJobDeploymentStatus is a gauge metric that indicates the current deployment status of the RayJob custom resources.
		rayJobDeploymentStatus: prometheus.NewDesc(
			"kuberay_job_deployment_status",
			"The RayJob's current deployment status",
			[]string{"name", "namespace", "deployment_status"},
			nil,
		),
		client:       client,
		log:          ctrl.LoggerFrom(ctx),
		cleanupQueue: make([]RayJobMetricCleanupItem, 0),
		metricTTL:    time.Duration(metricsTTLSeconds) * time.Second,
	}

	// Start the cleanup goroutine
	go manager.startRayJobCleanupLoop(ctx)
	return manager
}

// Describe implements prometheus.Collector interface Describe method.
func (r *RayJobMetricsManager) Describe(ch chan<- *prometheus.Desc) {
	r.rayJobExecutionDurationSeconds.Describe(ch)
	ch <- r.rayJobInfo
	ch <- r.rayJobDeploymentStatus
}

// Collect implements prometheus.Collector interface Collect method.
func (r *RayJobMetricsManager) Collect(ch chan<- prometheus.Metric) {
	r.rayJobExecutionDurationSeconds.Collect(ch)

	var rayJobList rayv1.RayJobList
	err := r.client.List(context.Background(), &rayJobList)
	if err != nil {
		r.log.Error(err, "Failed to list RayJob resources")
		return
	}

	for _, rayJob := range rayJobList.Items {
		r.collectRayJobInfo(&rayJob, ch)
		r.collectRayJobDeploymentStatus(&rayJob, ch)
	}
}

func (r *RayJobMetricsManager) ObserveRayJobExecutionDuration(name, namespace string, jobDeploymentStatus rayv1.JobDeploymentStatus, retryCount int, duration float64) {
	r.rayJobExecutionDurationSeconds.WithLabelValues(name, namespace, string(jobDeploymentStatus), strconv.Itoa(retryCount)).Set(duration)
}

// ScheduleRayJobMetricForCleanup schedules a RayJob metric for cleanup after the TTL
func (r *RayJobMetricsManager) ScheduleRayJobMetricForCleanup(name, namespace string) {
	r.queueMutex.Lock()
	defer r.queueMutex.Unlock()

	// Add to cleanup queue
	item := RayJobMetricCleanupItem{
		Name:      name,
		Namespace: namespace,
		DeleteAt:  time.Now().Add(r.metricTTL),
	}
	r.cleanupQueue = append(r.cleanupQueue, item)
	r.log.Info("Scheduled RayJob metric for cleanup", "name", name, "namespace", namespace, "deleteAt", item.DeleteAt)
}

// startRayJobCleanupLoop starts a loop to clean up expired RayJob metrics
func (r *RayJobMetricsManager) startRayJobCleanupLoop(ctx context.Context) {
	// Check for expired metrics every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanupExpiredRayJobMetrics()
		case <-ctx.Done():
			return
		}
	}
}

// cleanupExpiredRayJobMetrics removes RayJob metrics that have expired
func (r *RayJobMetricsManager) cleanupExpiredRayJobMetrics() {
	r.queueMutex.Lock()
	defer r.queueMutex.Unlock()

	now := time.Now()
	remainingItems := make([]RayJobMetricCleanupItem, 0)

	for _, item := range r.cleanupQueue {
		if now.After(item.DeleteAt) {
			// Remove all metrics associated with this job
			r.rayJobExecutionDurationSeconds.DeletePartialMatch(prometheus.Labels{
				"name":      item.Name,
				"namespace": item.Namespace,
			})
			r.log.Info("Cleaned up expired RayJob metrics", "name", item.Name, "namespace", item.Namespace)
		} else {
			// Keep non-expired items
			remainingItems = append(remainingItems, item)
		}
	}

	// Update queue
	r.cleanupQueue = remainingItems
}

func (r *RayJobMetricsManager) collectRayJobInfo(rayJob *rayv1.RayJob, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		r.rayJobInfo,
		prometheus.GaugeValue,
		1,
		rayJob.Name,
		rayJob.Namespace,
	)
}

func (r *RayJobMetricsManager) collectRayJobDeploymentStatus(rayJob *rayv1.RayJob, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		r.rayJobDeploymentStatus,
		prometheus.GaugeValue,
		1,
		rayJob.Name,
		rayJob.Namespace,
		string(rayJob.Status.JobDeploymentStatus),
	)
}
