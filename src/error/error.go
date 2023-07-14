/*======================
エラーハンドル
========================*/
package error

import (
	"fmt"
	"net/http"
	"os"

	LOG "bwing.app/src/log"
	"github.com/pkg/errors"
)

type OsSettingsStruct struct {
	Hostname string
}

var OsSettings OsSettingsStruct

type ErrorLogs struct {
	Error   error
	Host    string
	Method  string
	Urlpath string
	Headers http.Header
	Params  string
}

func init() {
	n, _ := os.Hostname()
	OsSettings.Hostname = n
	//fmt.Println(OsSettings)
}

//Error responseの箱
type ErrorResult struct {
	HttpStatus int
	Error      []string
}

///////////////////////////////////////////////////
/* ===========================================
//Errorの結果のロギング
err は、WithStackで包んで渡してくることを必須とする
例: errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
* =========================================== */
func ErrorLoggingWithStackTrace(err error) {

	//出力項目
	var output ErrorLogs = ErrorLogs{
		Error: err,
	}

	//Error logging with stacktrace
	fmt.Println(LOG.SetLogEntry(LOG.ERROR, "Errorlogging", fmt.Sprintf("%+v\n", output)))
}

///////////////////////////////////////////////////
/* ===========================================
////Errorの結果をレスポンス
* =========================================== */
func ErrorResponse(w http.ResponseWriter, err error, errStatus int) {

	//Status
	if errStatus == 0 {
		errStatus = http.StatusInternalServerError
	}

	//Severity:ERROR Error logging
	//fmt.Println(LOG.SetLogEntry(LOG.ERROR, "Errorlogging", fmt.Sprintf("%+v\n", err)))

	//Error logging with stacktrace
	ErrorLoggingWithStackTrace(errors.WithStack(fmt.Errorf("%d, %w", errStatus, err)))
}
