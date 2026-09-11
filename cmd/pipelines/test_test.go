package pipelines

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveWorkspaceTestSelectors(t *testing.T) {
	actual := resolveWorkspaceTestSelectors(
		"/Workspace/Users/test/.bundle/project/dev/files",
		[]string{
			"tests/test_orders.py::test_valid_order",
			"/Workspace/Shared/tests/test_shared.py",
		},
	)

	assert.Equal(t, []string{
		"/Workspace/Users/test/.bundle/project/dev/files/tests/test_orders.py::test_valid_order",
		"/Workspace/Shared/tests/test_shared.py",
	}, actual)
}

func TestLooksLikePytestSelector(t *testing.T) {
	assert.True(t, looksLikePytestSelector("tests/test_orders.py"))
	assert.True(t, looksLikePytestSelector("tests::test_order"))
	assert.False(t, looksLikePytestSelector("orders_pipeline"))
}

func TestWritePipelineTestJUnit(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "results.xml")
	result := pipelineTestResult{
		Summary: pipelineTestSummary{TotalCount: 2, PassedCount: 1, FailedCount: 1},
		Tests: []pipelineTestCaseProgress{
			{NodeID: "tests/test_orders.py::test_valid", Status: "TEST_CASE_PASSED", Stdout: "created order"},
			{
				NodeID:    "tests/test_orders.py::test_invalid",
				Status:    "TEST_CASE_FAILED",
				Message:   "assertion failed",
				Traceback: "expected invalid order",
			},
		},
	}

	require.NoError(t, writePipelineTestJUnit(filename, result))
	xmlDocument, err := os.ReadFile(filename)
	require.NoError(t, err)
	assert.Contains(t, string(xmlDocument), `tests="2"`)
	assert.Contains(t, string(xmlDocument), `failures="1"`)
	assert.Contains(t, string(xmlDocument), `<system-out>created order</system-out>`)
	assert.Contains(t, string(xmlDocument), `<failure message="assertion failed">expected invalid order</failure>`)
}
