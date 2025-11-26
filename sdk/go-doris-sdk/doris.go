// Package doris provides a high-level API for loading data into Apache Doris
// This is a backward-compatible wrapper that re-exports functionality from pkg/load
package doris

import "github.com/apache/doris/sdk/go-doris-sdk/pkg/load"

// Config aliases
type Config = load.Config

// Client aliases
type DorisLoadClient = load.DorisLoadClient

// Format aliases
type Format = load.Format
type JSONFormatType = load.JSONFormatType
type JSONFormat = load.JSONFormat
type CSVFormat = load.CSVFormat

// Log aliases
type LogLevel = load.LogLevel
type LogFunc = load.LogFunc
type ContextLogger = load.ContextLogger

// Load response aliases
type LoadResponse = load.LoadResponse
type LoadStatus = load.LoadStatus

// Enum constants
const (
	// JSON format constants
	JSONObjectLine = load.JSONObjectLine
	JSONArray      = load.JSONArray

	// Group commit constants
	SYNC  = load.SYNC
	ASYNC = load.ASYNC
	OFF   = load.OFF

	// Load status constants
	SUCCESS = load.SUCCESS
	FAILURE = load.FAILURE

	// Log level constants
	LogLevelDebug = load.LogLevelDebug
	LogLevelInfo  = load.LogLevelInfo
	LogLevelWarn  = load.LogLevelWarn
	LogLevelError = load.LogLevelError
)

// GroupCommitMode aliases
type GroupCommitMode = load.GroupCommitMode
type Retry = load.Retry

// Function aliases for easy access
var (
	// Client functions
	NewLoadClient = load.NewLoadClient

	// Data conversion helpers
	StringReader = load.StringReader
	BytesReader  = load.BytesReader
	JSONReader   = load.JSONReader

	// Logging functions
	SetLogLevel       = load.SetLogLevel
	SetLogOutput      = load.SetLogOutput
	DisableLogging    = load.DisableLogging
	SetCustomLogFunc  = load.SetCustomLogFunc
	SetCustomLogFuncs = load.SetCustomLogFuncs
	NewContextLogger  = load.NewContextLogger

	// Default configuration builders
	DefaultJSONFormat = load.DefaultJSONFormat
	DefaultCSVFormat  = load.DefaultCSVFormat
	DefaultRetry      = load.DefaultRetry
	NewRetry          = load.NewRetry
	NewDefaultRetry   = load.NewDefaultRetry
)
