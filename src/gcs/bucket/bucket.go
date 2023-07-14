/*
	=================================

共通bucketの構造
その他、bucketを扱う共通項目や処理
* =================================
*/
package bucket

import (
	"regexp"
	"strings"
)

var (
	ERR_MSG_BUCKE_DATA_DID_NOT_EXIST = "Bucket Data did not exist."
)

// Inerface
type Bucket struct{}

// /////////////////////////////////////////
// GCSから取得した結果を格納する箱
type LogssStruct struct {
	Objs    [][]string
	Paths   [][]string
	LogDate []string
	Len     int
}

///////////////////////////////////////////////////
/* ===========================================
// GCSロギングのログルーターログをinsertIdごとに文字列配列にする
=========================================== */
func SplitGcsLoggings2Array(log string) []string {
	//1時間毎のログから、個別のログを分割(抽出)
	lp := strings.Replace(log, "{\"insertId\":", "<|>{\"insertId\":", -1) //パイプで区切る
	lds := strings.Split(lp, "<|>")                                       //パイプをデリミタとして配列に分割
	lds = lds[1:]                                                         //パイプ置換で先頭が空要素になるため、これを除外する
	return lds
}

///////////////////////////////////////////////////
/* ===========================================
GCP Loggingのログ(文字列)からinsertIdを抜き出す
ARGS
	string;: 先頭にinsertId:のJSONキーがある一つのログ
RETURN
	string: insertId
=========================================== */
func GcsLoggingExtractParseErrorNoInsertId(log string) string {
	rexp := regexp.MustCompile("\\\"")
	str := strings.Split(log, ",")[0]
	str = rexp.ReplaceAllString(str, "")
	return str + ", {...}"
}
