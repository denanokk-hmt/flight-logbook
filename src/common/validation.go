/*
======================
共通Validation処理
========================
*/
package common

import (
	"regexp"
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
日付の確認
ARGS
	date:[string]:検査する日付文字列
RETURN
	検索結果(正:true/誤:false)
=========================================== */
func DateFormatChecker(d string) bool {

	//不要な文字列を削除
	reg := regexp.MustCompile(`[-|/|:| |　]`)
	str := reg.ReplaceAllString(d, "")

	//数値の値に変換するフォーマットを定義
	format := string([]rune("20060102150405")[:len(str)])

	//日付文字列をパースしてエラーが出ないかを確認
	_, err := time.Parse(format, str)
	return err == nil
}
