package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestMetricRayJobInfo(t *testing.T) {
	tests := []struct {
		name            string
		rayJobs         []rayv1.RayJob
		expectedMetrics []string
	}{
		{
			name: "two jobs and delete one later",
			rayJobs: []rayv1.RayJob{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-1",
						Namespace: "ns1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-2",
						Namespace: "ns2",
					},
				},
			},
			expectedMetrics: []string{
				`kuberay_job_info{name="ray-job-1",namespace="ns1"} 1`,
				`kuberay_job_info{name="ray-job-2",namespace="ns2"} 1`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(k8sScheme))

			objs := make([]client.Object, len(tc.rayJobs))
			for i := range tc.rayJobs {
				objs[i] = &tc.rayJobs[i]
			}
			client := fake.NewClientBuilder().WithScheme(k8sScheme).WithObjects(objs...).Build()
			manager := NewRayJobMetricsManager(context.Background(), client)
			reg := prometheus.NewRegistry()
			reg.MustRegister(manager)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
			require.NoError(t, err)
			rr := httptest.NewRecorder()
			handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
			handler.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, label := range tc.expectedMetrics {
				assert.Contains(t, body, label)
			}

			if len(tc.rayJobs) > 0 {
				err = client.Delete(t.Context(), &tc.rayJobs[0])
				require.NoError(t, err)
			}

			rr2 := httptest.NewRecorder()
			handler.ServeHTTP(rr2, req)

			assert.Equal(t, http.StatusOK, rr2.Code)
			body2 := rr2.Body.String()

			assert.NotContains(t, body2, tc.expectedMetrics[0])
			for _, label := range tc.expectedMetrics[1:] {
				assert.Contains(t, body2, label)
			}
		})
	}
}

func TestMetricRayJobDeploymentStatus(t *testing.T) {
	tests := []struct {
		name            string
		rayJobs         []rayv1.RayJob
		expectedMetrics []string
	}{
		{
			name: "two jobs with different deployment statuses",
			rayJobs: []rayv1.RayJob{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-1",
						Namespace: "ns1",
					},
					Status: rayv1.RayJobStatus{
						JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-2",
						Namespace: "ns2",
					},
					Status: rayv1.RayJobStatus{
						JobDeploymentStatus: rayv1.JobDeploymentStatusFailed,
					},
				},
			},
			expectedMetrics: []string{
				`kuberay_job_deployment_status{deployment_status="Running",name="ray-job-1",namespace="ns1"} 1`,
				`kuberay_job_deployment_status{deployment_status="Failed",name="ray-job-2",namespace="ns2"} 1`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(k8sScheme))

			objs := make([]client.Object, len(tc.rayJobs))
			for i := range tc.rayJobs {
				objs[i] = &tc.rayJobs[i]
			}
			client := fake.NewClientBuilder().WithScheme(k8sScheme).WithObjects(objs...).Build()
			manager := NewRayJobMetricsManager(context.Background(), client)
			reg := prometheus.NewRegistry()
			reg.MustRegister(manager)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
			require.NoError(t, err)
			rr := httptest.NewRecorder()
			handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
			handler.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, label := range tc.expectedMetrics {
				assert.Contains(t, body, label)
			}

			if len(tc.rayJobs) > 0 {
				err = client.Delete(t.Context(), &tc.rayJobs[0])
				require.NoError(t, err)
			}

			rr2 := httptest.NewRecorder()
			handler.ServeHTTP(rr2, req)

			assert.Equal(t, http.StatusOK, rr2.Code)
			body2 := rr2.Body.String()

			assert.NotContains(t, body2, tc.expectedMetrics[0])
			for _, label := range tc.expectedMetrics[1:] {
				assert.Contains(t, body2, label)
			}
		})
	}
}

// TestRayJobMetricCleanupItem tests the structure of RayJobMetricCleanupItem
func TestRayJobMetricCleanupItem(t *testing.T) {
	// Create a test cleanup item
	item := RayJobMetricCleanupItem{
		Name:      "test-job",
		Namespace: "default",
		DeleteAt:  time.Now().Add(5 * time.Minute),
	}

	// Verify the fields are correctly set
	assert.Equal(t, "test-job", item.Name)
	assert.Equal(t, "default", item.Namespace)
	assert.WithinDuration(t, time.Now().Add(5*time.Minute), item.DeleteAt, time.Second)
}

// TestScheduleRayJobMetricForCleanup tests adding items to the cleanup queue
func TestScheduleRayJobMetricForCleanup(t *testing.T) {
	// Create a fake client
	k8sScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(k8sScheme))
	client := fake.NewClientBuilder().WithScheme(k8sScheme).Build()
	// Create a metrics manager
	manager := NewRayJobMetricsManager(context.Background(), client)

	// Schedule a cleanup for a job
	manager.ScheduleRayJobMetricForCleanup("test-job", "default")

	// Verify the item was added to the queue
	manager.queueMutex.Lock()
	defer manager.queueMutex.Unlock()

	assert.Len(t, manager.cleanupQueue, 1)
	assert.Equal(t, "test-job", manager.cleanupQueue[0].Name)
	assert.Equal(t, "default", manager.cleanupQueue[0].Namespace)
	assert.WithinDuration(t, time.Now().Add(5*time.Minute), manager.cleanupQueue[0].DeleteAt, time.Second)
}

// TestCleanupExpiredRayJobMetrics tests the cleanup of expired metrics
func TestCleanupExpiredRayJobMetrics(t *testing.T) {
	// Create a registry, fake client and metrics manager
	registry := prometheus.NewRegistry()
	k8sScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(k8sScheme))
	client := fake.NewClientBuilder().WithScheme(k8sScheme).Build()
	manager := NewRayJobMetricsManager(context.Background(), client)

	// Register the manager with the registry
	registry.MustRegister(manager)

	// Record a metric for a job
	manager.ObserveRayJobExecutionDuration("test-job", "default", rayv1.JobDeploymentStatusComplete, 0, 10.5)

	// Verify the metric exists
	metrics, err := registry.Gather()
	require.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "kuberay_job_execution_duration_seconds", metrics[0].GetName())

	// Add the job to cleanup queue with a past delete time
	manager.queueMutex.Lock()
	manager.cleanupQueue = append(manager.cleanupQueue, RayJobMetricCleanupItem{
		Name:      "test-job",
		Namespace: "default",
		DeleteAt:  time.Now().Add(-1 * time.Minute), // Expired
	})
	manager.queueMutex.Unlock()

	// Run cleanup
	manager.cleanupExpiredRayJobMetrics()

	// Verify the metric was deleted
	metrics, err = registry.Gather()
	require.NoError(t, err)
	// The metric should still exist but have no samples
	assert.Len(t, metrics, 1)
	assert.Equal(t, 0, len(metrics[0].GetMetric()))
}

// TestRayJobCleanupLoop tests the background cleanup loop
func TestRayJobCleanupLoop(t *testing.T) {
	// Create a fake client and metrics manager
	k8sScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(k8sScheme))
	client := fake.NewClientBuilder().WithScheme(k8sScheme).Build()
	manager := NewRayJobMetricsManager(context.Background(), client)

	// Start the cleanup loop
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.startRayJobCleanupLoop(ctx)
	}()

	// Schedule a cleanup for a job
	manager.ScheduleRayJobMetricForCleanup("test-job", "default")

	// Wait for the TTL to expire and cleanup to run
	time.Sleep(2 * time.Second)

	// Verify the cleanup queue is empty
	manager.queueMutex.Lock()
	defer manager.queueMutex.Unlock()
	assert.Len(t, manager.cleanupQueue, 0)

	// Stop the cleanup loop
	cancel()
	wg.Wait()
}

// TestRayJobConditionProvisioned tests metrics when job is provisioned and cleaned up
func TestRayJobConditionProvisioned(t *testing.T) {
	// Create a registry, fake client and metrics manager
	registry := prometheus.NewRegistry()
	k8sScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(k8sScheme))
	client := fake.NewClientBuilder().WithScheme(k8sScheme).Build()
	manager := NewRayJobMetricsManager(context.Background(), client)
	registry.MustRegister(manager)

	// Simulate a job becoming provisioned
	job := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
		Status: rayv1.RayJobStatus{
			JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			JobStatus:           rayv1.JobStatusRunning,
			StartTime:           &metav1.Time{Time: time.Now().Add(-10 * time.Second)},
		},
	}

	// Simulate job completion and emit metrics
	oldStatus := job.Status
	job.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusComplete
	job.Status.JobStatus = rayv1.JobStatusSucceeded

	// Emit metrics and schedule cleanup
	emitRayJobExecutionDuration(manager, job.Name, job.Namespace, oldStatus, job.Status)

	// Verify the metric was recorded
	metrics, err := registry.Gather()
	require.NoError(t, err)
	assert.Len(t, metrics, 1)
	assert.Equal(t, "kuberay_job_execution_duration_seconds", metrics[0].GetName())

	// Fast-forward time and run cleanup
	manager.queueMutex.Lock()
	for i := range manager.cleanupQueue {
		manager.cleanupQueue[i].DeleteAt = time.Now().Add(-1 * time.Minute) // Force expire
	}
	manager.queueMutex.Unlock()

	manager.cleanupExpiredRayJobMetrics()

	// Verify the metric was cleaned up
	metrics, err = registry.Gather()
	require.NoError(t, err)
	assert.Len(t, metrics[0].GetMetric(), 0)
}

// Helper function to match the one in controller
func emitRayJobExecutionDuration(rayJobMetricsObserver RayJobMetricsObserver, rayJobName, rayJobNamespace string, originalRayJobStatus, rayJobStatus rayv1.RayJobStatus) {
	if rayJobStatus.StartTime == nil {
		// Set a default start time if not provided
		now := time.Now()
		rayJobStatus.StartTime = &metav1.Time{Time: now.Add(-10 * time.Second)}
	}
	if !rayv1.IsJobDeploymentTerminal(originalRayJobStatus.JobDeploymentStatus) && (rayv1.IsJobDeploymentTerminal(rayJobStatus.JobDeploymentStatus) ||
		rayJobStatus.JobDeploymentStatus == rayv1.JobDeploymentStatusRetrying) {

		retryCount := 0
		if originalRayJobStatus.Failed != nil {
			retryCount += int(*originalRayJobStatus.Failed)
		}

		rayJobMetricsObserver.ObserveRayJobExecutionDuration(
			rayJobName,
			rayJobNamespace,
			rayJobStatus.JobDeploymentStatus,
			retryCount,
			time.Since(rayJobStatus.StartTime.Time).Seconds(),
		)
	}
}
