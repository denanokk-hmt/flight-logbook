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

type ArgsTugcarApiRequest struct{}

///////////////////////////////////////////////////
/* ===========================================
コマンドライン引数で指定がなかった項目に、contentに応じたDefault値を格納
=========================================== */
func (a *ArgsTugcarApiRequest) AddDefault2Args(dvm *DVM) {

	/*-----------------------------------
	Content別にデフォルト値を準備
	-----------------------------------*/

	/////////////////////////
	//デフォルト値を設定(content別)

	//bucket
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_NAME] = BUCKET.GCS_BUCKET_NAME_TUGCAR_API_REQUEST_LOGGING
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_MIDDLE_PATH] = BUCKET.GCS_MIDDLE_PATH_TUGCAR_API_REQUEST_LOGGING

	//bigquery
	dvm.DefaultValueMap[CONFIG.ARGS_BQ_DATESET_NAME] = TABLE.DATASET_TUGCAR_API_REQUEST_LOGGING
	dvm.DefaultValueMap[CONFIG.ARGS_BQ_TABLE_NAME] = TABLE.TABLE_TUGCAR_API_REQUEST_LOGGING
}
