/*
======================
Logging
========================
*/
package log

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	INFO  = "INFO"
	WARN  = "WARNING"
	ERROR = "ERROR"
)

type LogEntry struct {
	Severity    string    `json:"severity"`    //ログレベル
	LogName     string    `json:"logName"`     //ログ名
	TextPayload string    `json:"textPayload"` //ログ内容
	Timestamp   time.Time `json:"timestamp"`   //ログタイムスタンプ
}

type OsSettingsStruct struct {
	Hostname string
}

var OsSettings OsSettingsStruct

type RequestLogs struct {
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

func init() {
	log.SetPrefix("") // 接頭辞の設定
}

///////////////////////////////////////////////////
/* ===========================================
//構造体をJSON形式の文字列へ変換
=========================================== */
func (l LogEntry) String() string {
	out, err := json.Marshal(l)
	if err != nil {
		log.Printf("json.Marshal: %v", err)
	}
	return string(out)
}

///////////////////////////////////////////////////
/* ===========================================
//ログエントリの箱につめる
=========================================== */
func SetLogEntry(level, logName, text string) string {
	entry := &LogEntry{
		Severity:    level,
		LogName:     logName,
		TextPayload: text,
		Timestamp:   time.Now(),
	}
	return entry.String()
}

///////////////////////////////////////////////////
/* ===========================================
簡易ロギング
=========================================== */
func JustLogging(text string) {
	fmt.Printf("Logging:[%s]\n", text)
}
