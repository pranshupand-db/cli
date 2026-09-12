package pipelines

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/cmd/bundle/utils"
	"github.com/databricks/cli/libs/auth"
	"github.com/databricks/cli/libs/cmdctx"
	"github.com/databricks/cli/libs/cmdio"
	databricks "github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/spf13/cobra"
)

type pipelineTestDetails struct {
	SourcePaths []string `json:"source_paths,omitempty"`
	Selectors   []string `json:"selectors,omitempty"`
	RunnerArgs  []string `json:"runner_args,omitempty"`
}

type startPipelineTestResponse struct {
	UpdateID string `json:"update_id"`
}

type pipelineTestCaseProgress struct {
	NodeID    string `json:"node_id"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	Traceback string `json:"traceback"`
	Stdout    string `json:"stdout"`
}

type pipelineTestSummary struct {
	TotalCount   int `json:"total_count"`
	PassedCount  int `json:"passed_count"`
	FailedCount  int `json:"failed_count"`
	SkippedCount int `json:"skipped_count"`
}

type pipelineTestEvent struct {
	Timestamp string                     `json:"timestamp"`
	Details   map[string]json.RawMessage `json:"details"`
}

type pipelineTestEventsResponse struct {
	Events []pipelineTestEvent `json:"events"`
}

type pipelineTestResult struct {
	UpdateID   string                     `json:"update_id"`
	State      pipelines.UpdateInfoState  `json:"state"`
	Summary    pipelineTestSummary        `json:"summary"`
	Tests      []pipelineTestCaseProgress `json:"tests"`
	HasSummary bool                       `json:"-"`
}

func testCommand() *cobra.Command {
	var sourcePaths []string
	var selectors []string
	var junitXML string
	var noWait bool
	var pipelineID string

	cmd := &cobra.Command{
		Use:   "test [KEY] [PYTEST_NODE...] -- [RUNNER_ARG...]",
		Short: "Run pipeline tests",
		Long: `Run tests against the code deployed with a pipeline.

Test nodes use pytest syntax, for example:
  databricks pipelines test orders tests/test_orders.py::test_valid_order

Arguments after -- are forwarded to the configured test runner:
  databricks pipelines test orders -- -k smoke --maxfail=1`,
		Args: cobra.ArbitraryArgs,
	}
	cmd.Flags().StringSliceVar(&sourcePaths, "source-path", nil, "Workspace paths containing deployed tests.")
	cmd.Flags().StringSliceVar(&selectors, "select", nil, "Pytest node selectors to run.")
	cmd.Flags().StringVar(&junitXML, "junit-xml", "", "Write test results to a local JUnit XML file.")
	cmd.Flags().BoolVar(&noWait, "no-wait", false, "Return after starting the test update.")
	cmd.Flags().StringVar(&pipelineID, "pipeline-id", "", "Run tests on a pipeline ID without bundle resolution.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		var runnerArgs []string
		beforeDash := args
		if dash := cmd.ArgsLenAtDash(); dash >= 0 {
			beforeDash = args[:dash]
			runnerArgs = args[dash:]
		}
		var w *databricks.WorkspaceClient
		var positionalSelectors []string
		if pipelineID != "" {
			w = cmdctx.WorkspaceClient(cmd.Context())
			positionalSelectors = beforeDash
		} else {
			b, err := utils.ProcessBundle(cmd, utils.ProcessOptions{InitIDs: true})
			if err != nil {
				return err
			}
			key, bundleSelectors, err := resolvePipelineTestArguments(
				cmd.Context(),
				b,
				args,
				cmd.ArgsLenAtDash(),
			)
			if err != nil {
				return err
			}
			pipelineID, err = resolvePipelineIdFromKey(cmd.Context(), b, key)
			if err != nil {
				return err
			}
			positionalSelectors = bundleSelectors
			positionalSelectors = resolveWorkspaceTestSelectors(b.Config.Workspace.FilePath, positionalSelectors)
			selectors = resolveWorkspaceTestSelectors(b.Config.Workspace.FilePath, selectors)
			if len(sourcePaths) == 0 {
				sourcePaths = []string{b.Config.Workspace.FilePath}
			}
			sourcePaths = resolveWorkspaceTestSelectors(b.Config.Workspace.FilePath, sourcePaths)
			w = b.WorkspaceClient(cmd.Context())
		}

		allSelectors := append(selectors, positionalSelectors...)
		updateID, err := startPipelineTest(cmd.Context(), w, pipelineID, pipelineTestDetails{
			SourcePaths: sourcePaths,
			Selectors:   allSelectors,
			RunnerArgs:  runnerArgs,
		})
		if err != nil {
			return err
		}
		if noWait {
			return cmdio.Render(cmd.Context(), startPipelineTestResponse{UpdateID: updateID})
		}

		result, err := waitForPipelineTest(cmd.Context(), w, pipelineID, updateID)
		if err != nil {
			return err
		}
		if junitXML != "" {
			if err := writePipelineTestJUnit(junitXML, result); err != nil {
				return err
			}
		}
		if err := cmdio.RenderWithTemplate(cmd.Context(), result, "", pipelineTestResultTemplate); err != nil {
			return err
		}
		if result.State != pipelines.UpdateInfoStateCompleted || result.Summary.FailedCount > 0 {
			return errors.New("pipeline tests failed")
		}
		return nil
	}
	return cmd
}

func resolvePipelineTestArguments(
	ctx context.Context,
	b *bundle.Bundle,
	args []string,
	argsLenAtDash int,
) (string, []string, error) {
	beforeDash := args
	if argsLenAtDash >= 0 {
		beforeDash = args[:argsLenAtDash]
	}
	if len(beforeDash) > 0 && !looksLikePytestSelector(beforeDash[0]) {
		return beforeDash[0], beforeDash[1:], nil
	}
	key, err := resolvePipelineArgument(ctx, b, nil)
	return key, beforeDash, err
}

func looksLikePytestSelector(arg string) bool {
	return strings.Contains(arg, "::") ||
		strings.HasSuffix(arg, ".py") ||
		strings.HasPrefix(arg, "/") ||
		strings.HasPrefix(arg, ".")
}

func resolveWorkspaceTestSelectors(workspaceRoot string, selectors []string) []string {
	resolved := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		parts := strings.Split(selector, "::")
		if !strings.HasPrefix(parts[0], "/") {
			parts[0] = path.Join(workspaceRoot, parts[0])
		}
		resolved = append(resolved, strings.Join(parts, "::"))
	}
	return resolved
}

func startPipelineTest(
	ctx context.Context,
	w *databricks.WorkspaceClient,
	pipelineID string,
	details pipelineTestDetails,
) (string, error) {
	apiClient, err := client.New(w.Config)
	if err != nil {
		return "", err
	}
	var response startPipelineTestResponse
	err = apiClient.Do(
		ctx,
		"POST",
		fmt.Sprintf("/api/2.0/pipelines/%s/updates", pipelineID),
		auth.WorkspaceIDHeaders(w.Config),
		map[string]any{
			"test_only": true,
			"test_details": map[string]any{
				"source_paths": details.SourcePaths,
				"selectors":    details.Selectors,
				"runner_args":  details.RunnerArgs,
			},
		},
		nil,
		&response,
	)
	if err != nil {
		return "", err
	}
	return response.UpdateID, nil
}

func waitForPipelineTest(
	ctx context.Context,
	w *databricks.WorkspaceClient,
	pipelineID string,
	updateID string,
) (pipelineTestResult, error) {
	for {
		update, err := w.Pipelines.GetUpdateByPipelineIdAndUpdateId(ctx, pipelineID, updateID)
		if err != nil {
			return pipelineTestResult{}, err
		}
		switch update.Update.State {
		case pipelines.UpdateInfoStateCompleted, pipelines.UpdateInfoStateFailed, pipelines.UpdateInfoStateCanceled:
			result, eventErr := waitForPipelineTestEvents(ctx, w, pipelineID, updateID)
			if eventErr != nil {
				return pipelineTestResult{}, eventErr
			}
			if update.Update.State == pipelines.UpdateInfoStateCanceled {
				return result, errors.New("pipeline test update canceled")
			}
			result.State = update.Update.State
			return result, nil
		default:
		}
		select {
		case <-ctx.Done():
			return pipelineTestResult{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func waitForPipelineTestEvents(
	ctx context.Context,
	w *databricks.WorkspaceClient,
	pipelineID string,
	updateID string,
) (pipelineTestResult, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		result, err := fetchPipelineTestResult(ctx, w, pipelineID, updateID)
		if err != nil {
			return pipelineTestResult{}, err
		}
		if result.HasSummary || time.Now().After(deadline) {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return pipelineTestResult{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func fetchPipelineTestResult(
	ctx context.Context,
	w *databricks.WorkspaceClient,
	pipelineID string,
	updateID string,
) (pipelineTestResult, error) {
	apiClient, err := client.New(w.Config)
	if err != nil {
		return pipelineTestResult{}, err
	}
	var response pipelineTestEventsResponse
	err = apiClient.Do(
		ctx,
		"GET",
		fmt.Sprintf("/api/2.0/pipelines/%s/events", pipelineID),
		auth.WorkspaceIDHeaders(w.Config),
		nil,
		map[string]string{
			"filter":   fmt.Sprintf("update_id = '%s' AND event_type in ('test_case_progress', 'test_summary')", updateID),
			"order_by": "timestamp asc",
		},
		&response,
	)
	if err != nil {
		return pipelineTestResult{}, err
	}
	result := pipelineTestResult{UpdateID: updateID}
	for _, event := range response.Events {
		if raw, ok := event.Details["test_case_progress"]; ok {
			var test pipelineTestCaseProgress
			if err := json.Unmarshal(raw, &test); err != nil {
				return pipelineTestResult{}, err
			}
			result.Tests = append(result.Tests, test)
		}
		if raw, ok := event.Details["test_summary"]; ok {
			if err := json.Unmarshal(raw, &result.Summary); err != nil {
				return pipelineTestResult{}, err
			}
			result.HasSummary = true
		}
	}
	return result, nil
}

const pipelineTestResultTemplate = `{{range .Tests}}{{if eq .Status "TEST_CASE_PASSED"}}PASS{{else if eq .Status "TEST_CASE_SKIPPED"}}SKIP{{else}}FAIL{{end}} {{.NodeID}}
{{if .Stdout}}{{.Stdout}}{{end}}{{if .Traceback}}{{.Traceback}}{{end}}{{end}}
{{.Summary.PassedCount}} passed, {{.Summary.FailedCount}} failed, {{.Summary.SkippedCount}} skipped
`

type junitTestSuites struct {
	XMLName  xml.Name         `xml:"testsuites"`
	Tests    int              `xml:"tests,attr"`
	Failures int              `xml:"failures,attr"`
	Skipped  int              `xml:"skipped,attr"`
	Suites   []junitTestSuite `xml:"testsuite"`
}

type junitTestSuite struct {
	Name      string          `xml:"name,attr"`
	Tests     int             `xml:"tests,attr"`
	Failures  int             `xml:"failures,attr"`
	Skipped   int             `xml:"skipped,attr"`
	TestCases []junitTestCase `xml:"testcase"`
}

type junitTestCase struct {
	Name      string        `xml:"name,attr"`
	Failure   *junitFailure `xml:"failure,omitempty"`
	Skipped   *struct{}     `xml:"skipped,omitempty"`
	SystemOut string        `xml:"system-out,omitempty"`
}

type junitFailure struct {
	Message string `xml:"message,attr,omitempty"`
	Body    string `xml:",chardata"`
}

func writePipelineTestJUnit(filename string, result pipelineTestResult) error {
	suite := junitTestSuite{
		Name:     "pipeline tests",
		Tests:    result.Summary.TotalCount,
		Failures: result.Summary.FailedCount,
		Skipped:  result.Summary.SkippedCount,
	}
	for _, test := range result.Tests {
		testCase := junitTestCase{Name: test.NodeID, SystemOut: test.Stdout}
		switch test.Status {
		case "TEST_CASE_FAILED":
			testCase.Failure = &junitFailure{Message: test.Message, Body: test.Traceback}
		case "TEST_CASE_SKIPPED":
			testCase.Skipped = &struct{}{}
		}
		suite.TestCases = append(suite.TestCases, testCase)
	}
	document := junitTestSuites{
		Tests:    result.Summary.TotalCount,
		Failures: result.Summary.FailedCount,
		Skipped:  result.Summary.SkippedCount,
		Suites:   []junitTestSuite{suite},
	}
	data, err := xml.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, append([]byte(xml.Header), data...), 0o644)
}
