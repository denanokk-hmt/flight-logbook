/*
======================
Cloud-Run Jobsからの
設定値を取得
設定値についての処理
========================
*/
package args

import (
	TABLE "bwing.app/src/bigquery/table"
	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"
)

type ArgsFlightReord struct{}

///////////////////////////////////////////////////
/* ===========================================
コマンドライン引数で指定がなかった項目に、contentに応じたDefault値を格納
=========================================== */
func (a *ArgsFlightReord) AddDefault2Args(dvm *DVM) {

	/*-----------------------------------
	Content別にデフォルト値を準備
	-----------------------------------*/

	/////////////////////////
	//デフォルト値を設定(content別)

	//bucket
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_NAME] = BUCKET.GCS_BUCKET_NAME_FLIGHT_RECORD
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_MIDDLE_PATH] = BUCKET.GCS_MIDDLE_PATH_FLIGHT_RECORD

	//bigquery
	dvm.DefaultValueMap[CONFIG.ARGS_BQ_DATESET_NAME] = TABLE.DATASET_FLIGHT_RECORD
	dvm.DefaultValueMap[CONFIG.ARGS_BQ_TABLE_NAME] = TABLE.TABLE_FRIGHT_RECORD
}
